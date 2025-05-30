package simplewaldb

import "crypto/rand"

// keyFromInt creates a key from an int.
func keyFromInt(i int) Key {
	var key Key
	key[3] = byte(i)
	key[2] = byte(i >> 8)
	key[1] = byte(i >> 16)
	key[0] = byte(i >> 24)
	return key
}

var testRecSeparator = func() recordSeparator {
	var rs recordSeparator
	must(rs.fromHex("00000000000000000000000000000000000000000000000000000000000000"))
	return rs
}()

// mustRandomKey generates a new random key.
func mustRandomKey() Key {
	var key Key
	rand.Read(key[:])
	return key
}
