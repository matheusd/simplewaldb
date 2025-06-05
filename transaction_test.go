package simplewaldb

import (
	"testing"

	"matheusd.com/depvendoredtestify/require"
)

// TestReadOnlyTablePut tests that Put() is not allowed on read-only tables.
func TestReadOnlyTablePut(t *testing.T) {
	tableName := TableKey("test")
	db := newTestDB(t, WithTables(tableName))

	txc, err := db.PrepareTx(WithReadTables(tableName))
	require.NoError(t, err)

	err = txc.RunTx(func(tx Tx) error {
		table, err := tx.Table(tableName)
		if err != nil {
			return err
		}
		return table.Put(Key{}, []byte{})
	})
	require.ErrorIs(t, err, ErrTableNotWritableInTx(tableName))
}

// TestTxFluentAPI tests the behavior of Tx's fluent API.
func TestTxFluentAPI(t *testing.T) {
	writeTable, readTable := TableKey("writet"), TableKey("readt")
	db := newTestDB(t, WithTables(writeTable, readTable))

	// Add some preset values to the reading table (also tests basic fluent
	// API).
	key1, val1 := Key{0: 1}, []byte{1024: 1}
	key2, val2 := Key{0: 2}, []byte{1024: 2}
	key99, val99 := Key{0: 99}, []byte{1024: 99}
	runTestTx(t, prepTestTx(t, db, WithWriteTables(readTable)), func(tx Tx) error {
		return tx.
			Put(readTable, key1, val1).
			Put(readTable, key2, val2).Err()
	})

	// Prepare the test transaction.
	readVal := make([]byte, 2048, 2048)
	emptyReadVal := make([]byte, len(readVal), cap(readVal))
	txc := prepTestTx(t, db, WithReadTables(readTable), WithWriteTables(writeTable))
	runTestTx(t, txc, func(tx Tx) error {
		// Ok chain of actions.
		err := tx.
			Read(readTable, key1, &readVal).
			Put(writeTable, key1, val1).
			Err()
		require.NoError(t, err)
		require.Equal(t, val1, readVal)
		return nil
	})

	// Clear to test reading did not happen.
	clear(readVal)
	readVal = readVal[:cap(readVal)]

	runTestTx(t, txc, func(tx Tx) error {
		// Errored trying to write a read-only table.
		err := tx.
			Put(readTable, key99, val99).
			Read("does-not-exist", key1, &readVal).
			Read(writeTable, key1, &readVal).
			Err()
		require.ErrorIs(t, err, ErrTableNotWritableInTx(readTable))
		require.Equal(t, emptyReadVal, readVal) // readVal not modified

		// Trying to get a value does not return it after an error.
		gotVal1 := tx.Get(readTable, key1)
		require.Nil(t, gotVal1)

		return nil
	})

	runTestTx(t, txc, func(tx Tx) error {
		// Same as prior test, but Switched order of errors to test the
		// first one is the one that is reported.
		err := tx.
			Read("does-not-exist", key1, &readVal).
			Put(readTable, key99, val99).
			Read(writeTable, key1, &readVal).
			Err()
		require.ErrorIs(t, err, ErrTableNotInTx("does-not-exist"))
		require.Equal(t, emptyReadVal, readVal) // readVal not modified
		return nil
	})

	// Ensure key99 is still not written (because Put() happens after
	// the error).
	runTestTx(t, txc, func(tx Tx) error {
		err := tx.
			Read(readTable, key99, &readVal).
			Err()
		require.ErrorIs(t, err, ErrKeyNotFound(key99))
		require.Equal(t, emptyReadVal, readVal) // readVal not modified
		return nil
	})
	runTestTx(t, txc, func(tx Tx) error {
		err := tx.
			Read(writeTable, key99, &readVal).
			Err()
		require.ErrorIs(t, err, ErrKeyNotFound(key99))
		require.Equal(t, emptyReadVal, readVal) // readVal not modified
		return nil
	})

	// Put() after a Get() should work as expected.
	runTestTx(t, txc, func(tx Tx) error {
		return tx.
			Put(writeTable, key2, tx.Get(readTable, key2)).
			Err()
	})

	// But not if the key does not exist.
	runTestTx(t, txc, func(tx Tx) error {
		err := tx.
			Put(writeTable, key99, tx.Get(readTable, key99)).
			Err()
		require.ErrorIs(t, err, ErrKeyNotFound(key99))
		return nil
	})

	// Values should be the expected ones.
	runTestTx(t, txc, func(tx Tx) error {
		require.Equal(t, val1, tx.Get(readTable, key1))
		require.Equal(t, val2, tx.Get(readTable, key2))
		require.Equal(t, val1, tx.Get(writeTable, key1))
		require.Equal(t, val2, tx.Get(writeTable, key2))
		require.False(t, tx.Exists(writeTable, key99))
		require.False(t, tx.Exists(readTable, key99))
		return tx.Err()
	})
}

// BenchmarkTxCfgRunTx benchmarks the overhead of calling RunTx.
func BenchmarkTxCfgRunTx(b *testing.B) {
	tableName := TableKey("test")
	db := newTestDB(b, WithTables(tableName))

	txc, err := db.PrepareTx(WithReadTables(tableName))
	require.NoError(b, err)

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		err := txc.RunTx(func(tx Tx) error {
			// Call a non-inlinable test function to check if Tx
			// escapes to heap in the simplest case.
			tx.notInlinableNop()
			return nil
		})
		require.NoError(b, err)
	}
}

// BenchmarkTxBeginEnd benchmarks the overhead of calling BeginTx/EndTx.
func BenchmarkTxBeginEnd(b *testing.B) {
	tableName := TableKey("test")
	db := newTestDB(b, WithTables(tableName))

	txc, err := db.PrepareTx(WithReadTables(tableName))
	require.NoError(b, err)

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		tx, err := db.BeginTx(txc)
		require.NoError(b, err)

		// Call a non-inlinable test function to check if Tx
		// escapes to heap in the simplest case.
		tx.notInlinableNop()

		err = db.EndTx(&tx)
		require.NoError(b, err)
	}
}
