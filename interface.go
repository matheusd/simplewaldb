package simplewaldb

// KeySize is the size of a key (in bytes).
const KeySize = 16

// Key is the key to entries in a table.
//
// This is used as a type alias to ensure any 16 byte array type can work as a
// key (including, for example, raw UUIDs).
type Key = [KeySize]byte

var emptyKey Key

// TableKey is the key of a table. This MUST only contain filesystem-safe
// characters.
type TableKey string

// Tables is syntax sugar to create a []TableKey.
func Tables(tbls ...TableKey) []TableKey { return tbls }
