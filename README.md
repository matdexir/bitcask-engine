# Bitcask Engine

A simple Bitcask-inspired key-value store written in Go.

## Features

- Persistent key-value storage using append-only data files
- In-memory key directory for fast lookups
- Support for put, get, and delete operations
- File rollover when data files reach a configurable size
- Index rebuilding on startup for crash recovery

## Project Structure

```
go.mod
engine/
    engine.go           # Main Bitcask engine implementation
    engine_test.go      # Engine unit tests
    file_entry.go       # File entry serialization/deserialization
    file_entry_test.go  # File entry tests
    keydir.go           # Key directory structure
```

## Usage

### Build

```sh
go build ./engine
```

### Run Tests

```sh
go test ./engine
```

### Example

```go
import "bitcask/engine"

func main() {
    db, err := engine.NewBistcaskEngine("/path/to/data")
    if err != nil {
        panic(err)
    }
    defer db.Close()

    err = db.Put("foo", "bar")
    val, err := db.Get("foo")
    db.Delete("foo")
}
```

## License

MIT