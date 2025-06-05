package simplewaldb

import (
	"context"
	"errors"
	"fmt"
	"log"
	"maps"
	"math/rand/v2"
	"os"
	"slices"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"
	"matheusd.com/depvendoredtestify/require"
)

// TestRandomRW tests writing and reading from multiple goroutines concurrently.
// Useful for running with -race and -memprofile.
func TestRandomRW(t *testing.T) {
	const MAXVALUES = 2000
	const READVALUES = 20
	const WRITEVALUES = 5
	const MAXVALUESIZE = 2000 // bytes

	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)

	// Replace with the code below to keep the data after the test.
	// rootDir := t.TempDir()
	rootDir, err := os.MkdirTemp("", "")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Root dir: %v", rootDir)

	tables := make([]TableKey, 20)
	for i := range tables {
		tables[i] = TableKey(fmt.Sprintf("%03d", i))
	}

	db, err := NewDB(
		WithRootDir(rootDir),
		WithTables(tables...),

		// Easy to find separator when hexdumping the test tables.
		WithSeparatorHex("00000000000000000000000000000000000000000000000000000000000000"),
	)
	if err != nil {
		t.Fatal(err)
	}

	// Write values to each table.
	txc, err := db.PrepareTx(WithWriteTables(tables...))
	require.NoError(t, err)
	tx, err := db.BeginTx(txc)
	require.NoError(t, err)
	rngReader := rand.NewChaCha8([32]byte{})
	buf := make([]byte, MAXVALUESIZE)
	for _, tabName := range tables {
		tab, err := tx.Table(tabName)
		if err != nil {
			t.Fatal(err)
		}
		nb := MAXVALUES/2 + rand.IntN(MAXVALUES/2)
		for i := 0; i < nb; i++ {
			rngReader.Read(buf)
			key := keyFromInt(i)
			if err := tab.Put(Key(key), buf[:rand.IntN(len(buf))]); err != nil {
				t.Fatal(err)
			}
		}
	}
	err = db.EndTx(&tx)
	require.NoError(t, err)

	randomTables := func() []TableKey {
		n := rand.IntN(len(tables))
		res := make(map[TableKey]struct{})
		for i := 0; i < n; {
			idx := rand.IntN(len(tables))
			if _, ok := res[tables[idx]]; ok {
				continue
			}
			res[tables[idx]] = struct{}{}
			i++
		}
		return slices.Collect(maps.Keys(res))
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	g, ctx := errgroup.WithContext(ctx)

	t.Logf("===== Starting...")

	// Start 20 readers.
	for i := 0; i < 20; i++ {
		g.Go(func() error {
			buf := make([]byte, MAXVALUESIZE)
			for ctx.Err() == nil {
				tables := randomTables()
				txc, err := db.PrepareTx(WithReadTables(tables...))
				if err != nil {
					return err
				}
				tx, err := db.BeginTx(txc)
				if err != nil {
					return err
				}
				if len(tables) > 0 {
					for i := 0; i < READVALUES; i++ {
						tabName := tables[rand.IntN(len(tables))]
						tab, err := tx.Table(tabName)
						key := keyFromInt(rand.IntN(MAXVALUES))
						_, err = tab.Read(Key(key), buf)
						if err != nil && !errors.Is(err, ErrKeyNotFound{}) {
							err = db.EndTx(&tx)
							return err
						}
					}
				}
				time.Sleep(time.Duration(rand.IntN(int(5 * time.Microsecond))))
				err = db.EndTx(&tx)
				if err != nil {
					return err
				}
			}
			return nil
		})
	}

	// Start 5 writers.
	for i := 0; i < 5; i++ {
		g.Go(func() error {
			buf := make([]byte, MAXVALUESIZE)
			rngReader := rand.NewChaCha8([32]byte{0: byte(i)})
			for ctx.Err() == nil {
				tables := randomTables()
				if len(tables) == 0 {
					continue
				}
				i := rand.IntN(len(tables))
				var readTables, writeTables []TableKey
				if i < len(tables) {
					readTables, writeTables = tables[:i], tables[i:]
				}
				txc, err := db.PrepareTx(
					WithReadTables(readTables...),
					WithWriteTables(writeTables...),
				)
				if err != nil {
					return err
				}
				tx, err := db.BeginTx(txc)
				if err != nil {
					return err
				}

				if len(readTables) > 0 {
					for i := 0; i < READVALUES; i++ {
						tabName := readTables[rand.IntN(len(readTables))]
						tab, err := tx.Table(tabName)
						key := keyFromInt(rand.IntN(MAXVALUES))
						_, err = tab.Read(Key(key), buf)
						if err != nil && !errors.Is(err, ErrKeyNotFound{}) {
							err = db.EndTx(&tx)
							return err
						}
					}
				}

				if len(writeTables) > 0 {
					for i := 0; i < WRITEVALUES; i++ {
						tabName := writeTables[rand.IntN(len(writeTables))]
						tab, err := tx.Table(tabName)
						key := keyFromInt(rand.IntN(MAXVALUES))
						rngReader.Read(buf)
						err = tab.Put(Key(key), buf[:rand.IntN(len(buf))])
						if err != nil {
							err = db.EndTx(&tx)
							return err
						}
					}
				}
				time.Sleep(time.Duration(rand.IntN(int(5 * time.Microsecond))))

				err = db.EndTx(&tx)
				if err != nil {
					return err
				}
			}
			return nil

		})
	}

	g.Go(func() error {
		select {
		case <-time.After(3 * time.Second):
			t.Logf("Cancelling...")
			cancel()
		case <-ctx.Done():
		}
		return nil
	})

	t.Logf("Running...")
	err = g.Wait()
	require.NoError(t, err)
}

func BenchmarkDBPut(b *testing.B) {
	tableName := TableKey("test")
	rngReader := rand.NewChaCha8([32]byte{})
	value := make([]byte, 1024)
	rngReader.Read(value)

	setupTest := func(b *testing.B) *TxConfig {
		rootDir := b.TempDir()
		db, err := NewDB(
			WithRootDir(rootDir),
			WithTables(tableName),
		)
		require.NoError(b, err)
		b.Cleanup(func() { db.Close() })

		txc, err := db.PrepareTx(WithWriteTables(tableName))
		require.NoError(b, err)
		return txc
	}

	tests := []struct {
		sameKey bool
		apiType string
	}{{
		sameKey: true,
		apiType: "table",
	}, {
		sameKey: true,
		apiType: "tx",
	}, {
		sameKey: false,
		apiType: "table",
	}, {
		sameKey: false,
		apiType: "tx",
	}}

	for _, tc := range tests {
		name := fmt.Sprintf("sameKey=%v,api=%v", tc.sameKey, tc.apiType)
		b.Run(name, func(b *testing.B) {
			var key Key
			rngReader.Read(key[:])

			txc := setupTest(b)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if !tc.sameKey {
					rngReader.Read(key[:])
				}

				err := txc.RunTx(func(tx Tx) error {
					if tc.apiType == "table" {
						tab, err := tx.Table(tableName)
						if err != nil {
							return err
						}

						return tab.Put(key, value)
					} else {
						tx.Put(tableName, key, value)
						return tx.Err()
					}
				})
				require.NoError(b, err)
			}
		})
	}
}
