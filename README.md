# simplewaldb: A Simple Prototyping DB

[![Go Reference](https://pkg.go.dev/badge/matheusd.com/simplewaldb.svg)](https://pkg.go.dev/matheusd.com/simplewaldb)

`simplewaldb` is a simple database system designed for easy prototyping of Go
applications, with minimal dependencies.

> [!CAUTION]
> This is alpha quality software, provided with NO WARRANTIES either express or
> implied. USE AT YOUR OWN RISK.

[CHANGELOG](CHANGELOG.md)

# Philosophy & Intended Use

The intended goal of this package is to provide a quick and dirty database
system, with minimal dependencies, useful for prototyping Go applications. It is
meant to provide reasonable performance and API, while keeping the feature set
small enough that users won't confuse it for a fully featured DB.

The on-disk layout is very simple and meant for making manual intervention
possible (and hopefully reasonably easy). One particular design decision is to
never erase data: records are always appended to data files, while index entries
are appended (NEVER overwritten).

This does mean this database trades off data storage against simplicity: in
particular, rewriting the same entry over and over again (even with the same
data) will cause storage consumption to grow.

The rationale for this is that wasting storage space at the prototyping stage of
an app is preferable to consuming too many CPU/memory resources and setup
complexity (by having to decide upfront on a DB technology, define schemas,
etc). Additional storage is the cheapest resource to acquire, and being able to
trace data back in the database (even if it needs to be done manually) is
better.

# Example usage

```go
func testDB() error {
    db, err := NewDB(
        WithRootDir("/tmp/testdb"),
        WithTables("table01", "table02"),

        // Easy to find separator when hexdumping the test tables. Replace with
        // your own and do not make it public.
        WithSeparatorHex("00000000000000000000000000000000000000000000000000000000000000"),
    )
    if err != nil { return err }

    // Prepare a transaction. Can read from table01, can write to table02.
    txc, err := db.PrepareTx([]TableKey{"table01"}, []TableKey{"table02"})
    if err != nil { return err }

    // Begin the transaction.
    tx, err := db.BeginTx(txc)
    if err != nil { return nil }

    var key Key
    var buf []byte = make([]byte, 1024)

    // Write a value to table02.
    tab02, err := tx.Write("table02")
    if err != nil { return err }
    if err := tab02.Put(key, buf); err != nil {
        return err 
    }
    
    // Read a value from table01.
    tab01, err := tx.Read("table01")
    if err != nil { return err }
    if err := tab01.Read(key, buf); err != nil {
        return err 
    }

    // Finish the transaction.
    if err := db.EndTx(txc); err != nil {
        return err
    }

    // Close the DB.
    return db.Close()
}

```

# Features

- Multi-reader, single-writer concurrency model.
- Per-table-set locking.

# TODO

- Add WAL for atomicity (maybe?)
- Add ability to backtrack entries
- Add backup/restore functions


# Changelog

