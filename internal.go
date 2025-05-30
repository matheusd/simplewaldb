package simplewaldb

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
)

const recordSeparatorSize = 32

// recordSeparator is the separator between records in data.
type recordSeparator [recordSeparatorSize]byte

func (rs *recordSeparator) fromHex(s string) error {
	if len(s) != (recordSeparatorSize-1)*2 {
		return errors.New("wrong string size")
	}
	if _, err := hex.Decode(rs[1:], []byte(s)); err != nil {
		return err
	}

	// First byte is always \n.
	rs[0] = 10
	return nil
}

// indexRecordSize is the size of an index record. An index record is:
// 16 bytes hex-encoded offset
// 1 byte space
// 16 bytes hex-encoded size
// 1 byte space
// 32 bytes hex-encoded key
// 1 byte line feed
const indexRecordSize = 8*2 + 1 + 8*2 + 1 + KeySize*2 + 1

// indexRecord is an entry in the index.
type indexRecord struct {
	offset int64
	size   int64
	key    Key
}

const spaceChar = byte(' ')
const lfChar = byte('\n')

// decode the entry from a buffer.
func (ir *indexRecord) decode(b []byte) error {
	if len(b) != indexRecordSize {
		return errors.New("index entry is wrong")
	}

	var auxArr [8]byte
	aux := auxArr[:]

	_, err := hex.Decode(aux, b[:16])
	if err != nil {
		return fmt.Errorf("wrong offset: %v", err)
	}
	ir.offset = int64(binary.BigEndian.Uint64(aux))

	b = b[16+1:]
	_, err = hex.Decode(aux, b[:16])
	if err != nil {
		return fmt.Errorf("wrong size: %v", err)
	}
	ir.size = int64(binary.BigEndian.Uint64(aux))

	b = b[16+1:]
	_, err = hex.Decode(ir.key[:], b[:32])
	if err != nil {
		return fmt.Errorf("wrong key: %v", err)
	}

	return nil
}

// indexRecordWriter writes index record entries.
type indexRecordWriter struct {
	buf []byte
	aux []byte // 8 bytes
}

// writeEntry writes the entry and returns the encoded buffer. The buffer must
// not be modified outside the writer.
func (irw *indexRecordWriter) writeEntry(ir *indexRecord) []byte {
	var i int
	binary.BigEndian.PutUint64(irw.aux, uint64(ir.offset))
	i += hex.Encode(irw.buf, irw.aux)

	irw.buf[i] = spaceChar
	i++ // Space

	binary.BigEndian.PutUint64(irw.aux, uint64(ir.size))
	i += hex.Encode(irw.buf[i:], irw.aux)

	irw.buf[i] = spaceChar
	i++ // Space

	i += hex.Encode(irw.buf[i:], ir.key[:])
	irw.buf[i] = lfChar

	return irw.buf
}

// newIndexRecordWriter initializes a new index record writer.
func newIndexRecordWriter() *indexRecordWriter {
	return &indexRecordWriter{
		buf: make([]byte, indexRecordSize),
		aux: make([]byte, 8),
	}
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
