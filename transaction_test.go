package simplewaldb

import (
	"errors"
	"testing"
)

// TestReadOnlyTablePut tests that Put() is not allowed on read-only tables.
func TestReadOnlyTablePut(t *testing.T) {
	rootDir := t.TempDir()
	tableName := TableKey("test")
	db, err := NewDB(
		WithRootDir(rootDir),
		WithTables(tableName),
	)
	if err != nil {
		t.Fatal(err)
	}

	txc, err := db.PrepareTx(Tables(tableName), nil)
	if err != nil {
		t.Fatal(err)
	}

	err = txc.RunTx(func(tx Tx) error {
		table, err := tx.Read(tableName)
		if err != nil {
			return err
		}
		return table.Put(Key{}, []byte{})
	})
	if !errors.Is(err, ErrTableNotWritableInTx(tableName)) {
		t.Fatalf("Unexpected error: %v", err)
	}
}

// BenchmarkTxCfgRunTx benchmarks the overhead of calling RunTx.
func BenchmarkTxCfgRunTx(b *testing.B) {
	rootDir := b.TempDir()
	db, err := NewDB(
		WithRootDir(rootDir),
	)
	if err != nil {
		b.Fatal(err)
	}

	txc, err := db.PrepareTx(nil, nil)
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		txc.RunTx(func(tx Tx) error {
			// Just to ensure tx is kept.
			if tx.done {
				return errors.New("should not have been done")
			}
			return nil
		})
	}
}
