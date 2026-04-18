//
//  StreamAccumulator.swift
//  RsyncProcessStreaming
//
//  Created by Thomas Evensen on 09/01/2026.
//

import Foundation

/// Thread-safe accumulator for streaming output and error lines.
///
/// This actor manages the accumulation of stdout and stderr data, handling partial lines
/// and providing thread-safe access to accumulated output. It's designed for efficient
/// streaming with minimal memory overhead.
///
/// The accumulator handles line breaking intelligently, preserving partial lines across
/// multiple `consume()` calls until a newline is received.
actor StreamAccumulator {
    private var lines: [String] = []
    private var partialLine: String = ""
    private var errorLines: [String] = []
    private var lineCounter: Int = 0
    private let lineSeparator = "\n"
    private let carriageReturn = "\r"

    /// Consumes text data and returns any complete lines.
    ///
    /// Lines are delimited by `\n`, `\r\n`, or standalone `\r`.
    /// Standalone `\r` is treated as a line break to support rsync `--progress`
    /// output, which uses carriage return to overwrite the current line.
    /// Partial lines (text without a trailing delimiter) are buffered internally
    /// and combined with the next chunk of data. Empty lines are filtered out.
    ///
    /// - Parameter text: Raw text data from stdout
    /// - Returns: Array of complete, non-empty lines extracted from the text
    func consume(_ text: String) -> [String] {
        var newLines: [String] = []
        var buffer = partialLine

        for char in text {
            if char == "\n" {
                // \r\n case: strip trailing \r
                if buffer.hasSuffix("\r") {
                    buffer.removeLast()
                }
                if !buffer.isEmpty {
                    newLines.append(buffer)
                }
                buffer = ""
            } else if char == "\r" {
                // Standalone \r: treat as line break (rsync --progress output).
                // If a \n follows immediately, the \n branch above handles the \r\n pair
                // because the \r will have been appended to buffer and stripped there.
                // But for standalone \r (no following \n), we emit the line now.
                if !buffer.isEmpty {
                    newLines.append(buffer)
                }
                buffer = ""
            } else {
                buffer.append(char)
            }
        }

        partialLine = buffer
        lines.append(contentsOf: newLines)
        return newLines
    }

    /// Flushes any remaining partial line as a complete line.
    ///
    /// Call this at the end of processing to capture output that didn't end with a newline.
    ///
    /// - Returns: The trailing partial line, or nil if there was none
    func flushTrailing() -> String? {
        guard !partialLine.isEmpty else { return nil }
        let trailing = partialLine
        partialLine = ""
        lines.append(trailing)
        return trailing
    }

    /// Returns a snapshot of all accumulated output lines.
    func snapshot() -> [String] { lines }

    /// Records an error message from stderr.
    ///
    /// - Parameter text: Error text from stderr
    func recordError(_ text: String) {
        errorLines.append(text.trimmingCharacters(in: .whitespacesAndNewlines))
    }

    /// Returns a snapshot of all accumulated error lines.
    func errorSnapshot() -> [String] { errorLines }

    /// Increments and returns the line counter.
    ///
    /// - Returns: The new line count after incrementing
    func incrementLineCounter() -> Int {
        lineCounter += 1
        return lineCounter
    }

    /// Returns the current line count without incrementing.
    func getLineCount() -> Int { lineCounter }

    /// Resets all accumulated state to initial values.
    func reset() {
        lines.removeAll()
        partialLine = ""
        errorLines.removeAll()
        lineCounter = 0
    }
}
