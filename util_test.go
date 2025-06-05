package simplewaldb

import (
	"crypto/rand"
	"testing"

	"matheusd.com/depvendoredtestify/require"
)

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

// newTestDB initializes a new test DB with the given options.
func newTestDB(t testing.TB, opts ...Option) *DB {
	rootDir := t.TempDir()
	db, err := NewDB(
		append([]Option{WithRootDir(rootDir)}, opts...)...,
	)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	return db
}

// runTestTx runs a transaction and tests it did not error.
func runTestTx(t testing.TB, txc *TxConfig, f func(tx Tx) error) {
	err := txc.RunTx(f)
	require.NoError(t, err)
}

func prepTestTx(t testing.TB, db *DB, opts ...TxOption) *TxConfig {
	txc, err := db.PrepareTx(opts...)
	require.NoError(t, err)
	return txc
}
