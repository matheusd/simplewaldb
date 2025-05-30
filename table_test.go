package simplewaldb

import (
	"bytes"
	"math/rand/v2"
	"testing"
)

// TestTableCorrectness tests basic table operation correctness.
func TestTableCorrectness(t *testing.T) {
	const MAXVALUES = 1000

	rootDir := t.TempDir()
	tableName := TableKey("test")

	tab, err := newTable(rootDir, tableName, testRecSeparator)
	if err != nil {
		t.Fatal(err)
	}

	// Write values.
	buf := make([]byte, 1024)
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
		values[keys[i]] = append(make([]byte, 0, cap(buf)), buf[:n]...) // make same cap
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
	if err := tab.close(); err != nil {
		t.Fatal(err)
	}

	// Reopen.
	tab, err = newTable(rootDir, tableName, testRecSeparator)
	if err != nil {
		t.Fatal(err)
	}

	// Read random values.
	for range MAXVALUES * 4 {
		idx := rand.IntN(MAXVALUES)
		key := keys[idx]
		rngReader.Read(buf)
		n, err := tab.read(key, buf)
		if err != nil {
			t.Fatal(err)
		}
		value := values[key]
		if n != len(value) {
			t.Fatalf("Unexpected read size: got %d, want %d", n, len(value))
		}
		if !bytes.Equal(value, buf[:n]) {
			t.Fatalf("Value does not match expected")
		}
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
