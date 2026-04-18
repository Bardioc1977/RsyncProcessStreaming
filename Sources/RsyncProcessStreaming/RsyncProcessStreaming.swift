import Foundation
import OSLog

/// Errors that can occur during rsync process execution.
public enum RsyncProcessError: Error, LocalizedError {
    /// The rsync executable was not found at the specified path.
    case executableNotFound(String)

    /// The rsync process failed with a non-zero exit code.
    ///
    /// - Parameters:
    ///   - exitCode: The exit code returned by the process
    ///   - errors: Error messages captured from stderr
    case processFailed(exitCode: Int32, errors: [String])

    /// The process was cancelled by calling `cancel()`.
    case processCancelled

    /// The process timed out.
    case timeout(TimeInterval)

    /// The process is in an invalid state for the requested operation.
    case invalidState(ProcessState)

    public var errorDescription: String? {
        switch self {
        case let .executableNotFound(path):
            return "Rsync executable not found at path: \(path)"
        case let .processFailed(code, errors):
            let message = errors.joined(separator: "\n")
            return "rsync exited with code \(code).\n\(message)"
        case .processCancelled:
            return "Process was cancelled"
        case let .timeout(interval):
            return "Process timed out after \(interval) seconds"
        case let .invalidState(state):
            return "Process is in invalid state: \(state)"
        }
    }
}

/// Process state for lifecycle management.
public enum ProcessState: CustomStringConvertible, Sendable {
    case idle
    case running
    case cancelling
    case terminating
    case terminated(exitCode: Int32)
    case failed(Error)

    public var description: String {
        switch self {
        case .idle: "idle"
        case .running: "running"
        case .cancelling: "cancelling"
        case .terminating: "terminating"
        case let .terminated(code): "terminated(\(code))"
        case let .failed(error): "failed(\(error.localizedDescription))"
        }
    }
}

/// MainActor-isolated process manager for executing rsync with real-time output streaming.
///
/// Example usage:
/// ```swift
/// let handlers = ProcessHandlers(processTermination: { output, _ in
///     print("Completed with \(output?.count ?? 0) lines")
/// }, fileHandler: { _ in }, rsyncPath: nil, checkLineForError: { _ in },
/// updateProcess: { _ in }, propagateError: { print($0) }, checkForErrorInRsyncOutput: true)
///
/// let process = RsyncProcess(arguments: ["-av", "source/", "dest/"], handlers: handlers)
/// try process.executeProcess()
/// ```
@MainActor
public final class RsyncProcess {
    private let arguments: [String]
    private let hiddenID: Int?
    private let handlers: ProcessHandlers
    private let useFileHandler: Bool
    private let usePty: Bool
    private let timeoutInterval: TimeInterval?
    private var accumulator = StreamAccumulator()

    // MainActor-isolated state
    private var currentProcess: Process?
    private var timeoutTimer: Timer?
    private var state: ProcessState = .idle
    private var cancelled = false
    private var errorOccurred = false

    // pty file descriptors (only used when usePty is true)
    private var ptyPrimary: Int32 = -1
    private var ptyReadSource: DispatchSourceRead?

    /// Creates a new RsyncProcess instance.
    ///
    /// - Parameters:
    ///   - arguments: Command-line arguments to pass to rsync
    ///   - hiddenID: Optional identifier passed through to termination handler
    ///   - handlers: Configuration for all process callbacks and behaviors
    ///   - useFileHandler: Whether to invoke fileHandler callback for each line (default: false)
    ///   - timeout: Optional timeout interval in seconds after which the process will be terminated
    ///   - usePty: Use a pseudo-terminal for stdout instead of a pipe. Forces rsync
    ///     into line-buffered mode so that `--progress` output arrives in real time
    ///     instead of being held in stdio buffers. Default: false.
    public init(
        arguments: [String],
        hiddenID: Int? = nil,
        handlers: ProcessHandlers,
        useFileHandler: Bool = false,
        timeout: TimeInterval? = nil,
        usePty: Bool = false
    ) {
        // Validate arguments
        precondition(!arguments.isEmpty, "Arguments cannot be empty")
        if let rsyncPath = handlers.rsyncPath {
            precondition(!rsyncPath.isEmpty, "Rsync path cannot be empty if provided")
        }

        self.arguments = arguments
        self.hiddenID = hiddenID
        self.handlers = handlers
        self.useFileHandler = useFileHandler
        self.usePty = usePty
        timeoutInterval = timeout

        Logger.process.debugMessage("RsyncProcessStreaming initialized with \(arguments.count) arguments, usePty=\(usePty)")
    }

    /// Executes the rsync process with configured arguments and streams output.
    ///
    /// - Throws: `RsyncProcessError.executableNotFound` or `RsyncProcessError.invalidState`
    public func executeProcess() throws {
        guard case .idle = state else {
            throw RsyncProcessError.invalidState(state)
        }

        cancelled = false
        errorOccurred = false
        state = .running
        accumulator = StreamAccumulator()

        let executablePath = handlers.rsyncPath ?? "/usr/bin/rsync"
        guard FileManager.default.isExecutableFile(atPath: executablePath) else {
            state = .failed(RsyncProcessError.executableNotFound(executablePath))
            throw RsyncProcessError.executableNotFound(executablePath)
        }

        let process = Process()
        process.executableURL = URL(fileURLWithPath: executablePath)
        process.arguments = arguments
        process.environment = handlers.environment

        let errorPipe = Pipe()
        process.standardError = errorPipe

        if usePty {
            try setupPtyOutput(process: process)
        } else {
            let outputPipe = Pipe()
            process.standardOutput = outputPipe
            setupPipeHandlers(outputPipe: outputPipe, errorPipe: errorPipe)
            setupTerminationHandler(process: process, outputPipe: outputPipe, errorPipe: errorPipe)
        }

        if usePty {
            setupPtyErrorHandler(errorPipe: errorPipe)
            setupPtyTerminationHandler(process: process, errorPipe: errorPipe)
        }

        currentProcess = process
        handlers.updateProcess(process)

        startTimeoutTimer()

        try process.run()
        logProcessStart(process)
    }

    /// Cancels the running process.
    public func cancel() {
        guard !cancelled else { return }

        cancelled = true
        state = .cancelling

        timeoutTimer?.invalidate()
        timeoutTimer = nil

        if let process = currentProcess {
            process.terminate()
        }

        Logger.process.debugMessage("RsyncProcessStreaming:  Process cancelled")
        handlers.propagateError(RsyncProcessError.processCancelled)
    }

    /// Whether the process has been cancelled.
    public var isCancelledState: Bool { cancelled }

    /// Whether the process is currently running.
    public var isRunning: Bool { currentProcess?.isRunning ?? false }

    /// The current process state.
    public var currentState: ProcessState { state }

    /// The process identifier if running.
    public var processIdentifier: Int32? { currentProcess?.processIdentifier }

    /// The termination status if terminated.
    public var terminationStatus: Int32? { currentProcess?.terminationStatus }

    /// Returns a description of the command being executed.
    public var commandDescription: String {
        let executable = handlers.rsyncPath ?? "/usr/bin/rsync"
        let args = arguments.joined(separator: " ")
        return "\(executable) \(args)"
    }

    // MARK: - Private Setup Methods

    private func setupPipeHandlers(outputPipe: Pipe, errorPipe: Pipe) {
        outputPipe.fileHandleForReading.readabilityHandler = { [weak self] handle in
            let data = handle.availableData
            guard !data.isEmpty else { return }

            Task { @MainActor in
                guard let self, !self.cancelled, !self.errorOccurred else { return }
                if let text = String(data: data, encoding: .utf8), !text.isEmpty {
                    await self.handleOutputData(text)
                }
            }
        }

        errorPipe.fileHandleForReading.readabilityHandler = { [weak self] handle in
            let data = handle.availableData
            guard !data.isEmpty else { return }

            Task { @MainActor in
                guard let self else { return }
                if let text = String(data: data, encoding: .utf8), !text.isEmpty {
                    await self.accumulator.recordError(text)
                }
            }
        }
    }

    private func setupTerminationHandler(process: Process, outputPipe: Pipe, errorPipe: Pipe) {
        let queue = DispatchQueue(label: "com.rsync.process.termination", qos: .userInitiated)

        process.terminationHandler = { [weak self] task in
            queue.async {
                Thread.sleep(forTimeInterval: 0.05)

                let outputData = Self.drainPipe(outputPipe.fileHandleForReading)
                let errorData = Self.drainPipe(errorPipe.fileHandleForReading)

                outputPipe.fileHandleForReading.readabilityHandler = nil
                errorPipe.fileHandleForReading.readabilityHandler = nil

                Task { @MainActor in
                    guard let self else { return }
                    await self.processFinalOutput(
                        finalOutputData: outputData,
                        finalErrorData: errorData,
                        task: task
                    )
                }
            }
        }
    }

    private nonisolated static func drainPipe(_ fileHandle: FileHandle) -> Data {
        var allData = Data()
        while true {
            let data = fileHandle.availableData
            if data.isEmpty { break }
            allData.append(data)
        }
        return allData
    }

    private func startTimeoutTimer() {
        guard let timeout = timeoutInterval, timeout > 0 else { return }

        timeoutTimer = Timer.scheduledTimer(withTimeInterval: timeout, repeats: false) { [weak self] _ in
            Task { @MainActor in
                self?.handleTimeout()
            }
        }
    }

    private func handleTimeout() {
        guard !cancelled, !errorOccurred, case .running = state else { return }

        let timeout = timeoutInterval ?? 0
        Logger.process.debugMessage("RsyncProcessStreaming:  Timeout after \(timeout)s")

        let timeoutError = RsyncProcessError.timeout(timeout)
        state = .failed(timeoutError)
        cancelled = true
        currentProcess?.terminate()
        handlers.propagateError(timeoutError)
    }

    private func logProcessStart(_ process: Process) {
        guard let path = process.executableURL, let arguments = process.arguments else { return }
        Logger.process.debugWithThreadInfo("RsyncProcessStreaming:  COMMAND - \(path)")
        Logger.process.debugMessage("RsyncProcessStreaming:  ARGUMENTS - \(arguments.joined(separator: "\n"))")

        if let timeout = timeoutInterval {
            Logger.process.debugMessage("RsyncProcessStreaming:  Timeout set to \(timeout) seconds")
        }
    }

    // MARK: - Private Processing Methods

    private func processFinalOutput(
        finalOutputData: Data,
        finalErrorData: Data,
        task: Process
    ) async {
        timeoutTimer?.invalidate()
        timeoutTimer = nil

        if let text = String(data: finalOutputData, encoding: .utf8), !text.isEmpty {
            await handleOutputData(text)
        }

        if let trailing = await accumulator.flushTrailing() {
            Logger.process.debugMessage("RsyncProcessStreaming:  Flushed trailing: \(trailing)")
            await processOutputLine(trailing)
        }

        if let errorText = String(data: finalErrorData, encoding: .utf8), !errorText.isEmpty {
            await accumulator.recordError(errorText)
        }

        await handleTermination(task: task)
    }

    private func handleOutputData(_ text: String) async {
        guard !cancelled, !errorOccurred else { return }

        let lines = await accumulator.consume(text)
        guard !lines.isEmpty else { return }

        for line in lines {
            if cancelled || errorOccurred { break }
            await processOutputLine(line)
        }
    }

    private func processOutputLine(_ line: String) async {
        guard !cancelled, !errorOccurred else { return }

        if useFileHandler {
            let count = await accumulator.incrementLineCounter()
            handlers.fileHandler(count)
        }

        do {
            try handlers.checkLineForError(line)
        } catch {
            errorOccurred = true
            state = .failed(error)
            let msg = error.localizedDescription
            Logger.process.debugMessage("RsyncProcessStreaming:  Output error - \(msg)")
            currentProcess?.terminate()
            handlers.propagateError(error)
        }
    }

    private func handleTermination(task: Process) async {
        let output = await accumulator.snapshot()
        let errors = await accumulator.errorSnapshot()

        defer { cleanupProcess() }

        if cancelled {
            Logger.process.debugMessage("RsyncProcessStreaming:  Terminated due to cancellation")
            state = .terminated(exitCode: task.terminationStatus)
            handlers.processTermination(output, hiddenID)
            handlers.updateProcess(nil)
            return
        }

        // Priority 3: Handle process failure based on exit code
        if task.terminationStatus != 0, handlers.checkForErrorInRsyncOutput, !errorOccurred {
            let error = RsyncProcessError.processFailed(
                exitCode: task.terminationStatus,
                errors: errors
            )
            state = .failed(error)
            Logger.process.debugMessage(
                "RsyncProcessStreaming:  Process failed with exit code \(task.terminationStatus)"
            )

            handlers.propagateError(error)
        } else {
            state = .terminated(exitCode: task.terminationStatus)
        }

        // Always call termination handler
        handlers.processTermination(output, hiddenID)
        handlers.updateProcess(nil)
    }

    // MARK: - PTY Support

    /// Allocates a pseudo-terminal and assigns the replica side to the process's stdout.
    ///
    /// Uses `cfmakeraw()` for unmodified byte pass-through (no ONLCR, no ICANON).
    /// The primary side is read via DispatchSource on the **main queue** so that
    /// event handler and termination drain never race on the same fd.
    private func setupPtyOutput(process: Process) throws {
        var primary: Int32 = 0
        var replica: Int32 = 0

        guard openpty(&primary, &replica, nil, nil, nil) == 0 else {
            throw RsyncProcessError.executableNotFound("Failed to allocate pty")
        }

        self.ptyPrimary = primary

        // Raw mode: no echo, no ONLCR, no ICANON — bytes pass through unmodified.
        var raw = Darwin.termios()
        tcgetattr(primary, &raw)
        cfmakeraw(&raw)
        tcsetattr(primary, TCSANOW, &raw)

        // Non-blocking so reads on the main queue never stall the RunLoop.
        let flags = fcntl(primary, F_GETFL)
        if flags >= 0 {
            _ = fcntl(primary, F_SETFL, flags | O_NONBLOCK)
        }

        // Assign replica (child side) to process stdout.
        process.standardOutput = FileHandle(fileDescriptor: replica, closeOnDealloc: true)

        // DispatchSource on .main — event handler runs on the main thread.
        let source = DispatchSource.makeReadSource(fileDescriptor: primary, queue: .main)
        source.setEventHandler { [weak self] in
            guard let self, !self.cancelled, !self.errorOccurred else { return }

            let bufferSize = 16384
            let buffer = UnsafeMutablePointer<UInt8>.allocate(capacity: bufferSize)
            defer { buffer.deallocate() }

            // Read all available data in a loop (non-blocking fd).
            var chunk = Data()
            while true {
                let n = read(primary, buffer, bufferSize)
                if n <= 0 { break }
                chunk.append(buffer, count: n)
            }
            guard !chunk.isEmpty else { return }

            if let text = String(data: chunk, encoding: .utf8), !text.isEmpty {
                Task { @MainActor in
                    await self.handleOutputData(text)
                }
            }
        }
        // Cancel handler does NOT close the fd — termination handler closes after drain.
        source.resume()
        self.ptyReadSource = source
    }

    private func setupPtyErrorHandler(errorPipe: Pipe) {
        errorPipe.fileHandleForReading.readabilityHandler = { [weak self] handle in
            let data = handle.availableData
            guard !data.isEmpty else { return }

            Task { @MainActor in
                guard let self else { return }
                if let text = String(data: data, encoding: .utf8), !text.isEmpty {
                    await self.accumulator.recordError(text)
                }
            }
        }
    }

    /// Termination handler for pty mode.
    ///
    /// Cancels the DispatchSource, then performs drain + final processing on `.main`
    /// to avoid racing with the source's event handler on the same fd.
    private func setupPtyTerminationHandler(process: Process, errorPipe: Pipe) {
        let primaryFD = self.ptyPrimary
        let readSource = self.ptyReadSource

        process.terminationHandler = { [weak self] task in
            // 1. Cancel the source — no more event handler invocations.
            readSource?.cancel()

            // 2. Drain error pipe on a background thread (separate fd, no race).
            let errorData = Self.drainPipe(errorPipe.fileHandleForReading)
            errorPipe.fileHandleForReading.readabilityHandler = nil

            // 3. After a short delay, drain + process on .main.
            //    This ensures any in-flight DispatchSource Tasks have run first.
            DispatchQueue.main.asyncAfter(deadline: .now() + 0.05) {
                // Drain remaining pty data (fd is non-blocking, still open).
                let bufferSize = 65536
                let buffer = UnsafeMutablePointer<UInt8>.allocate(capacity: bufferSize)
                defer { buffer.deallocate() }
                var drainedData = Data()
                while true {
                    let n = read(primaryFD, buffer, bufferSize)
                    if n <= 0 { break }
                    drainedData.append(buffer, count: n)
                }

                // Now close the fd.
                close(primaryFD)

                Task { @MainActor in
                    guard let self else { return }
                    self.ptyReadSource = nil
                    await self.processFinalOutput(
                        finalOutputData: drainedData,
                        finalErrorData: errorData,
                        task: task
                    )
                }
            }
        }
    }

    private func cleanupProcess() {
        timeoutTimer?.invalidate()
        timeoutTimer = nil
        if let source = ptyReadSource {
            source.cancel()
            close(ptyPrimary)
            ptyReadSource = nil
        }
        currentProcess = nil
        state = .idle
    }

    nonisolated deinit {
        Logger.process.debugMessage("RsyncProcessStreaming:  DEINIT")
    }
}
