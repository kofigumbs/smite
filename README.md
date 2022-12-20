# Smite

Single-file wrapper for embedding SQLite in Swift.

- Ensures statements are prepared and executed in a thread-safe context
- Throws SQLite errors as Swift exceptions
- Auto-converts between native SQLite and Swift types

```swift
let db = try Smite(path: ":memory:")
print(try db.execute("select CURRENT_TIMESTAMP")) // [["CURRENT_TIMESTAMP": Optional("2022-12-20 03:46:05")]]
```

## Installation

Copy [Smite.swift](./Smite.swift) into your source code, or add it as a Git submodule:

```
git submodule add https://github.com/kofigumbs/smite Sources/Smite
```
