# Changelog

# v0.4.0

- Encode separator as hex instead of binary in data file
- Track the offset of index entries (allows reverse iteration over old key values)
- Add fluent API to ease transaction updates

# v0.3.0

- Switched `Key` to a type alias

# v0.2.1

- Fixed `TxTable.Put()` callable with read only tables
- Added `Table()` and `MustTable()` to `Tx`

# v0.2.0

- Added `TxTable.Count()
- Added `TxConfig.RunTx()`
