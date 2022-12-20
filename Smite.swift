//===----------------------------------------------------------------------===//
// MIT License
// 
// Copyright (c) 2022 Kofi Gumbs
// 
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
// 
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
// 
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
//===----------------------------------------------------------------------===//

import Foundation
import SQLite3

/// Single-file wrapper for embedding SQLite in Swift.
///
/// - Ensures statements are prepared and executed in a thread-safe context
/// - Throws SQLite errors as Swift exceptions
/// - Auto-converts between native SQLite and Swift types
///
/// ```swift
/// let db = try Smite(path: ":memory:")
/// print(try db.execute("select CURRENT_TIMESTAMP")) // [["CURRENT_TIMESTAMP": Optional("2022-12-20 03:46:05")]]
/// ```
///
public final class Smite {

    /// Represents any of the allowed types:
    ///
    /// | SQLite  | Swift                 |
    /// | ------- | --------------------- |
    /// | TEXT    | String                |
    /// | REAL    | Float, Double, etc.   |
    /// | INTEGER | Bool, Int, UInt, etc. |
    /// | NULL    | nil                   |
    ///
    /// Using any other Swift or SQLite type will throw an error.
    ///
    public typealias Value = Any?

    /// Wraps a SQLite error.
    ///
    public enum Error: Swift.Error, Equatable, CustomStringConvertible {
        case onOpen(sqliteCode: Int32, path: String)
        case onStep(sqliteCode: Int32)
        case onPrepare(sqliteCode: Int32, sql: String)
        case onBind(sqliteCode: Int32, index: Int32)
        case onBindType(index: Int32)
        case onColumnType(index: Int32)

        public var description: String {
            switch self {
            case let .onOpen(sqliteCode, path):
                return "Smite.Error.onOpen(\(Error.string(sqliteCode)), path: \(path.debugDescription))"
            case let .onStep(sqliteCode):
                return "Smite.Error.onStep(\(Error.string(sqliteCode))"
            case let .onPrepare(sqliteCode, sql):
                return "Smite.Error.onPrepare(\(Error.string(sqliteCode)), sql: \(sql.debugDescription))"
            case let .onBind(sqliteCode, index):
                return "Smite.Error.onBind(\(Error.string(sqliteCode)), index: \(index))"
            case let .onBindType(index):
                return "Smite.Error.onBindType(index: \(index))"
            case let .onColumnType(index):
                return "Smite.Error.onColumnType(index: \(index))"
            }
        }

        internal static func string(_ sqliteCode: Int32) -> String {
            return String(cString: sqlite3_errstr(sqliteCode)).debugDescription
        }
    }

    private let connection: OpaquePointer
    private let queue: DispatchQueue

    /// Opens the SQLite database at _path_, creating it if it doesn't exist.
    /// Database connection is closed when the object is deallocated.
    ///
    public init(path: String) throws {
        var rawConnection: OpaquePointer?
        let flags = SQLITE_OPEN_CREATE | SQLITE_OPEN_READWRITE | SQLITE_OPEN_FULLMUTEX
        let result = sqlite3_open_v2(path, &rawConnection, flags, nil)
        guard result == SQLITE_OK, let connection = rawConnection else {
            _ = sqlite3_close(rawConnection)
            throw Error.onOpen(sqliteCode: result, path: path)
        }
        self.connection = connection
        self.queue = DispatchQueue(label: "sqlite://\(path)", qos: .default, attributes: [], autoreleaseFrequency: .workItem, target: .global())
    }

    deinit {
        queue.sync {
            _ = sqlite3_close(connection)
        }
    }

    /// Executes one or several SQL statements, separated by semi-colons.
    ///
    public func execute(_ sql: String, arguments: Array<Value> = []) throws -> [[String: Value]] {
        return try queue.sync {
            var rawStatement: OpaquePointer?
            let result = sqlite3_prepare_v2(connection, sql, -1, &rawStatement, nil)
            defer {
                sqlite3_finalize(rawStatement)
            }
            guard result == SQLITE_OK, let statement = rawStatement else {
                throw Error.onPrepare(sqliteCode: result, sql: sql)
            }
            for (i, value) in arguments.enumerated() {
                try bind(statement: statement, value: value, index: Int32(i+1))
            }
            return try evaluate(statement: statement)
        }
    }

    private func bind(statement: OpaquePointer, value: Value, index: Int32) throws {
        let result: Int32
        switch value {
        case let value as String:
            let transient = unsafeBitCast(-1, to: sqlite3_destructor_type.self)
            result = sqlite3_bind_text(statement, index, value, -1, transient)
        case let value as NSNumber where CFNumberIsFloatType(value):
            result = sqlite3_bind_double(statement, index, value.doubleValue)
        case let value as NSNumber:
            result = sqlite3_bind_int64(statement, index, value.int64Value)
        case nil, is NSNull:
            result = sqlite3_bind_null(statement, index)
        default:
            throw Error.onBindType(index: index)
        }
        guard result == SQLITE_OK else {
            throw Error.onBind(sqliteCode: result, index: index)
        }
    }

    private func evaluate(statement: OpaquePointer) throws -> [[String: Value]] {
        var output = [[String: Value]]()
        while true {
            switch sqlite3_step(statement) {
            case SQLITE_ROW:
                output.append(try nextRow(statement: statement))
            case SQLITE_DONE:
                return output
            case let result:
                throw Error.onStep(sqliteCode: result)
            }
        }
    }

    private func nextRow(statement: OpaquePointer) throws -> [String: Value] {
        var row = [String: Value]()
        for index in 0 ..< sqlite3_column_count(statement) {
            let name = String(cString: sqlite3_column_name(statement, index))
            let value = try nextColumn(statement: statement, index: index)
            row[name] = value
        }
        return row
    }

    private func nextColumn(statement: OpaquePointer, index: Int32) throws -> Value {
        switch sqlite3_column_type(statement, index) {
        case SQLITE_TEXT:
            return String(cString: sqlite3_column_text(statement, index))
        case SQLITE_FLOAT:
            return sqlite3_column_double(statement, index)
        case SQLITE_INTEGER:
            return sqlite3_column_int64(statement, index)
        case SQLITE_NULL:
            return nil
        default:
            throw Error.onColumnType(index: index)
        }
    }
}
