package simplewaldb

import (
	"math/rand/v2"
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
	if err != nil {
		t.Fatal(err)
	}

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
