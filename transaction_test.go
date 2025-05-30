package simplewaldb

import (
	"errors"
	"testing"
)

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
