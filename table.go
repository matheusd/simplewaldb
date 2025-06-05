package simplewaldb

import (
	"bufio"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
)

// table is a single table in the database.
type table struct {
	key TableKey
	sep recordSeparator

	// sepBuffer is a buffer to write the key and separator.
	sepBuffer []byte

	// irw is the writer of index records.
	irw *indexRecordWriter

	dataFile  *os.File
	indexFile *os.File

	// index maps an entry code
	index map[Key]*indexRecord
}

// close closes the table.
func (tab *table) close() error {
	err1 := tab.dataFile.Close()
	err2 := tab.indexFile.Close()
	if err1 != nil {
		return err1
	}
	return err2
}

// readEntry reads a data entry from the file.
func (tab *table) readEntry(entry *indexRecord, buf []byte) (int, error) {
	if int64(len(buf)) > entry.size {
		buf = buf[:entry.size]
	}

	n, err := tab.dataFile.ReadAt(buf, entry.offset)
	if err != nil {
		return n, fmt.Errorf("failed to read data entry: %v", err)
	}

	return n, nil
}

// read a data entry from the table into the buffer.
func (tab *table) read(key Key, buf []byte) (int, error) {
	entry, ok := tab.index[key]
	if !ok {
		return 0, ErrKeyNotFound{}
	}

	return tab.readEntry(entry, buf)
}

// count returns the number of items in the table.
func (tab *table) count() int {
	return len(tab.index)
}

// exists returns true if the given key is set in the table.
func (tab *table) exists(key Key) bool {
	_, ok := tab.index[key]
	return ok
}

// get returns the data of the key as a new slice.
func (tab *table) get(key Key) ([]byte, error) {
	entry, ok := tab.index[key]
	if !ok {
		return nil, ErrKeyNotFound(key)
	}

	data := make([]byte, entry.size)
	n, err := tab.readEntry(entry, data)
	if err != nil {
		return nil, fmt.Errorf("failed to read data entry: %v", err)
	}
	if n != len(data) {
		return nil, fmt.Errorf("short read: read %d, expected %d", n, len(data))
	}

	return data, nil
}

// put appends the data for the specified key to the table. This is NOT safe
// for concurrent calls.
func (tab *table) put(key Key, data []byte) error {
	// Encode the key into the temp buffer (separator is already there).
	hex.Encode(tab.sepBuffer[recordSeparatorSize:], key[:])

	// Get current end of data file to determine offset
	offset, err := tab.dataFile.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}

	// Get current end of index file to determine index offset
	indexOffset, err := tab.indexFile.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}

	// Write the data.
	n, err := tab.dataFile.Write(data)
	if err != nil {
		return err
	}
	if n != len(data) {
		return errors.New("short write")
	}

	// Write the separator.
	n, err = tab.dataFile.Write(tab.sepBuffer)
	if err != nil {
		return err
	}
	if n != len(tab.sepBuffer) {
		return errors.New("short write")
	}

	// Commit.
	if err := tab.dataFile.Sync(); err != nil {
		return fmt.Errorf("error fsyncing data table: %v", err)
	}

	// Store entry in memory index
	var entry *indexRecord
	if entry = tab.index[key]; entry == nil {
		entry = &indexRecord{
			key:             key,
			offset:          offset,
			size:            int64(len(data)),
			prevIndexOffset: math.MaxInt64,
			indexOffset:     indexOffset,
		}
		tab.index[key] = entry
	} else {
		entry.offset = offset
		entry.size = int64(len(data))
		entry.prevIndexOffset = entry.indexOffset
		entry.indexOffset = indexOffset
	}

	// Append entry to indexFile.
	irBuf := tab.irw.writeEntry(entry)
	_, err = tab.indexFile.Write(irBuf)
	if err != nil {
		return fmt.Errorf("error while writing index record: %v", err)
	}
	if err := tab.indexFile.Sync(); err != nil {
		return fmt.Errorf("error fsyncing index table: %v", err)
	}

	return nil // Indicate success
}

// newTable creates or opens an existing table.
func newTable(rootDir string, tableName TableKey, recSep recordSeparator) (*table, error) {
	// TODO: lock files?

	// Open the files.
	dataPath := filepath.Join(rootDir, string(tableName)+".data")
	dataFile, err := os.OpenFile(dataPath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}

	indexPath := filepath.Join(rootDir, string(tableName)+".index")
	indexFile, err := os.OpenFile(indexPath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		dataFile.Close() // Close dataFile if indexFile fails to open
		return nil, err
	}

	// Read the index into memory.
	index := make(map[Key]*indexRecord)
	indexReader := bufio.NewReader(indexFile)
	irBuf := make([]byte, indexRecordSize)
	var indexOffset int64
	for i := 0; ; i++ {
		var n int
		_, err = io.ReadFull(indexReader, irBuf)
		if err != nil {
			break
		}

		entry := new(indexRecord)
		if err := entry.decode(irBuf); err != nil {
			return nil, fmt.Errorf("error reading index entry %d: %v", i, err)
		}
		entry.indexOffset, indexOffset = indexOffset, indexOffset+int64(n)

		index[entry.key] = entry
	}

	sepBuffer := make([]byte, KeySize*2+recordSeparatorSize+8) // +8 padding
	for i := range sepBuffer {
		sepBuffer[i] = lfChar
	}
	copy(sepBuffer, recSep[:])

	return &table{
		key:       tableName,
		dataFile:  dataFile,
		indexFile: indexFile,
		index:     index,
		sepBuffer: sepBuffer,
		irw:       newIndexRecordWriter(),
	}, nil
}
