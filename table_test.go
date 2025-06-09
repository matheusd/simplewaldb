package simplewaldb

import (
	"bytes"
	"errors"
	"math/rand/v2"
	"slices"
	"testing"

	"matheusd.com/depvendoredtestify/require"
)

// TestTableCorrectness tests basic table operation correctness.
func TestTableCorrectness(t *testing.T) {
	const MAXVALUES = 1000
	const MAXVALUESIZE = 1024

	rootDir := t.TempDir()
	tableName := TableKey("test")

	tab, err := newTable(rootDir, tableName, testRecSeparator)
	require.NoError(t, err)

	// Write values.
	buf := make([]byte, MAXVALUESIZE, MAXVALUESIZE)
	keys := make([]Key, MAXVALUES)
	values := make(map[Key][]byte)
	rngReader := rand.NewChaCha8([32]byte{})
	for i := range MAXVALUES {
		n := rand.IntN(cap(buf))
		rngReader.Read(keys[i][:])
		rngReader.Read(buf[:n])
		err := tab.put(keys[i], buf[:n])
		if err != nil {
			t.Fatal(err)
		}

		// Clone buf but use same cap.
		bufCopy := append(make([]byte, 0, cap(buf)), buf[:n]...)
		values[keys[i]] = bufCopy
	}

	// Overwrite some values.
	for range MAXVALUES / 2 {
		n := rand.IntN(cap(buf))
		idx := rand.IntN(MAXVALUES)
		key := keys[idx]
		buf := values[key][:n]
		rngReader.Read(buf)
		values[key] = buf // Track last size
		err := tab.put(key, buf)
		if err != nil {
			t.Fatal(err)
		}
	}

	// Close the table.
	require.NoError(t, tab.close())

	// Reopen.
	tab, err = newTable(rootDir, tableName, testRecSeparator)
	require.NoError(t, err)

	// Read random values.
	for range MAXVALUES * 4 {
		idx := rand.IntN(MAXVALUES)
		key := keys[idx]
		clear(buf)
		n, err := tab.read(key, buf)
		require.NoError(t, err)
		value := values[key]
		require.Equal(t, len(value), n)
		require.Equal(t, value, buf[:n])
	}
}

// TestTabRangeRevEntries tests reverse ranging over table entries.
func TestTabRangeRevEntries(t *testing.T) {
	const MAXVALUES = 1000
	const MAXVALUESIZE = 1024

	rngReader := rand.NewChaCha8([32]byte{})
	buf := make([]byte, MAXVALUESIZE, MAXVALUESIZE)

	// Pick a key to trace over the range.
	var checkKey Key
	rngReader.Read(checkKey[:])
	var checkValues [][]byte

	rootDir := t.TempDir()
	tableName := TableKey("test")

	tab, err := newTable(rootDir, tableName, testRecSeparator)
	require.NoError(t, err)

	// Write a bunch of values.
	for range MAXVALUES {
		// 10% of the values will be on checkKey. The others will be
		// randomly written.
		var key Key
		writeCheckKey := rand.IntN(10) == 0
		if writeCheckKey {
			key = checkKey
		} else {
			rngReader.Read(key[:])
		}

		n := rand.IntN(cap(buf))
		rngReader.Read(buf[:n])
		err := tab.put(key, buf[:n])
		require.NoError(t, err)

		// Clone buf but use same cap.
		if writeCheckKey {
			bufCopy := slices.Clone(buf[:n])
			checkValues = append(checkValues, bufCopy)
		}
	}

	// Range backwards over the keys, checking the values. Do it twice: once
	// with the table still open, the second time after reopening.
	for range 2 {
		var wantValueIdx = len(checkValues) - 1
		err := tab.rangeRevEntries(checkKey, func(ir indexRecord) error {
			n, err := tab.readEntry(&ir, buf)
			if err != nil {
				return err
			}
			if n != int(ir.size) {
				return errors.New("read size is smaller than entry size")
			}

			if !bytes.Equal(buf[:n], checkValues[wantValueIdx]) {
				return errors.New("value is wrong")
			}

			wantValueIdx--
			return nil
		})

		// Range should've completed successfully.
		require.NoError(t, err)
		require.Equal(t, -1, wantValueIdx)

		// Close and reopen for next iteration.
		require.NoError(t, tab.close())
		tab, err = newTable(rootDir, tableName, testRecSeparator)
		require.NoError(t, err)
	}
}

// BenchmarkTablePutSameKey benchmarks putting the same key over and over.
func BenchmarkTablePutSameKey(b *testing.B) {
	b.ReportAllocs()

	rootDir := b.TempDir()
	tableName := TableKey("test")

	tab, err := newTable(rootDir, tableName, testRecSeparator)
	if err != nil {
		b.Fatal(err)
	}
	buf := make([]byte, 1024)
	key := mustRandomKey()
	rngReader := rand.NewChaCha8([32]byte{})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rngReader.Read(buf)
		err := tab.put(key, buf)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkTablePutDiffKey benchmarks putting different keys over and over.
func BenchmarTablekDiffKey(b *testing.B) {
	b.ReportAllocs()

	rootDir := b.TempDir()
	tableName := TableKey("test")

	tab, err := newTable(rootDir, tableName, testRecSeparator)
	if err != nil {
		b.Fatal(err)
	}
	buf := make([]byte, 1024)
	key := mustRandomKey()
	rngReader := rand.NewChaCha8([32]byte{})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rngReader.Read(buf)
		rngReader.Read(key[:])
		err := tab.put(key, buf)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkTableRead benchmarks reading keys from the table.
func BenchmarkRead(b *testing.B) {
	const MAXVALUES = 1000
	b.ReportAllocs()

	rootDir := b.TempDir()
	tableName := TableKey("test")

	tab, err := newTable(rootDir, tableName, testRecSeparator)
	if err != nil {
		b.Fatal(err)
	}
	buf := make([]byte, 1024)
	keys := make([]Key, MAXVALUES)
	rngReader := rand.NewChaCha8([32]byte{})
	for i := range MAXVALUES {
		rngReader.Read(keys[i][:])
		rngReader.Read(buf)
		err := tab.put(keys[i], buf)
		if err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rngReader.Read(buf)
		idx := rand.IntN(MAXVALUES)
		_, err := tab.read(keys[idx], buf)
		if err != nil {
			b.Fatal(err)
		}
	}
}
