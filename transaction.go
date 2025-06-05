package simplewaldb

import (
	"errors"
	"fmt"
	"sort"
	"sync"
)

// txTableCfg holds the config for a single table transaction.
type txTableCfg struct {
	table    *table
	key      TableKey
	writable bool
	lock     *sync.RWMutex
}

// TxConfig defines a prepared tx configuration.
type TxConfig struct {
	db        *DB
	lockOrder []*txTableCfg
	tables    map[TableKey]*txTableCfg
}

// RunTx runs the given function as a transaction. It ends the transaction after
// f returns.
//
// The transaction reference passed in the function is NOT safe for concurrent
// access and MUST NOT be kept after f returns.
func (txc *TxConfig) RunTx(f func(tx Tx) error) error {
	tx, err := txc.db.BeginTx(txc)
	if err != nil {
		return err
	}

	err = f(tx)
	endErr := txc.db.EndTx(&tx)
	if err != nil {
		return err
	}
	return endErr
}

// TxTable is a table obtained within the context of a transaction. Operations
// on the table are only valid while the transaction is active.
type TxTable struct {
	writable bool
	tab      *table
	tx       *Tx
}

// IsWritable returns true if this table is writable.
func (tt *TxTable) IsWritable() bool {
	return tt.writable
}

// Read a record from the table into the buffer. This reads at most len(buf)
// bytes from the entry, therefore the buffer should be sized appropriately.
func (tt *TxTable) Read(key Key, buf []byte) (int, error) {
	return tt.tab.read(key, buf)
}

// Get a record from the table as a new byte slice. This reads the entire record.
// Prefer using Read() when the size (or upper bound) of the record is known to
// avoid having to perform an allocation.
func (tt *TxTable) Get(key Key) ([]byte, error) {
	if tt.tx.done {
		return nil, ErrTxDone
	}

	return tt.tab.get(key)
}

// Put a record into the table.
//
// NOTE: Put calls are immediately written to the filesystem. The DB does NOT
// support atomicity across multiple tables within a transaction.
func (tt *TxTable) Put(key Key, data []byte) error {
	if tt.tx.done {
		return ErrTxDone
	}
	if !tt.writable {
		return ErrTableNotWritableInTx(tt.tab.key)
	}

	return tt.tab.put(key, data)
}

// Count returns the number of items in the table.
func (tt *TxTable) Count() (int, error) {
	if tt.tx.done {
		return 0, ErrTxDone
	}

	return tt.tab.count(), nil
}

// Tx is an open transaction in the DB. A transaction is NOT safe for
// concurrent access by multiple goroutines.
//
// A Tx objects offers several methods as part of a fluent-like API: these
// methods return either a reference to Tx itself or (in some situations) a
// value without an error.
//
// These methods all check for whether the tx has _already_ errored before
// causing (most) side-effects. If the method itself errors, it sets the
// internal error flag.
//
// This allows writing chains of transaction operations and only performing a
// single error check at the end.
type Tx struct {
	done bool
	err  error
	cfg  *TxConfig
}

func (tx *Tx) setErr(err error) error {
	tx.err = err
	return err
}

// Err returns the first error recorded by the transaction.
func (tx *Tx) Err() error {
	return tx.err
}

// notInlinableNop is a simple test function.
//
//go:noinline
func (tx *Tx) notInlinableNop() {
	if tx == nil {
		panic("nil tx")
	}
}

// Table returns the given table.
//
// The table will be writable or not depending on how it was configured in the
// transaction.
//
// Note: this is NOT part of the Tx's fluent API and does NOT set the internal
// error flag if it errors. This function may be used to check whether a table
// was added to the tx and its state.
func (tx *Tx) Table(key TableKey) (TxTable, error) {
	if tx.done {
		return TxTable{}, ErrTxDone
	}
	tc, ok := tx.cfg.tables[key]
	if !ok {
		return TxTable{}, ErrTableNotInTx(key)
	}
	return TxTable{tab: tc.table, tx: tx, writable: tc.writable}, nil
}

// MustTable returns the given table or panics. This should only be called with
// hardcoded table names that are known to be in the transaction and a not done
// tx.
//
// The table will be writable or not depending on how it was configured.
func (tx *Tx) MustTable(key TableKey) TxTable {
	tt, err := tx.Table(key)
	if err != nil {
		panic(err)
	}
	return tt
}

// Exists returns true if the transaction has not errored and the given key
// exists in the given table.
//
// This is part of Tx's fluent API.
func (tx *Tx) Exists(table TableKey, key Key) bool {
	if tx.done || tx.err != nil {
		return false
	}
	tc, ok := tx.cfg.tables[table]
	if !ok {
		tx.setErr(ErrTableNotInTx(table))
		return false
	}

	return tc.table.exists(key)
}

// Read a table value into a slice. The slice SHOULD NOT be nil and its length
// will be modified to the read length.
//
// This is part of Tx's fluent API.
func (tx *Tx) Read(table TableKey, key Key, value *[]byte) *Tx {
	if tx.done || tx.err != nil {
		return tx
	}
	tc, ok := tx.cfg.tables[table]
	if !ok {
		tx.setErr(ErrTableNotInTx(table))
		return tx
	}
	if value == nil {
		tx.setErr(errors.New("cannot read into nil slice"))
		return tx
	}

	n, err := tc.table.read(key, *value)
	if err != nil {
		tx.setErr(err)
		return tx
	}

	// Replace with new slice.
	*value = (*value)[:n]
	return tx
}

// Get returns a slice with the given table value. The slice is only non-nil if
// the tx has not errored and the value exists in the table.
//
// This is part of Tx's fluent API.
func (tx *Tx) Get(table TableKey, key Key) []byte {
	if tx.done || tx.err != nil {
		return nil
	}
	tc, ok := tx.cfg.tables[table]
	if !ok {
		tx.setErr(ErrTableNotInTx(table))
		return nil
	}

	v, err := tc.table.get(key)
	if err != nil {
		tx.setErr(err)
		return nil
	}

	return v
}

// Put the given value in the table.
//
// This is part of Tx's fluent API.
func (tx *Tx) Put(table TableKey, key Key, value []byte) *Tx {
	if tx.done || tx.err != nil {
		return tx
	}
	tc, ok := tx.cfg.tables[table]
	if !ok {
		tx.setErr(ErrTableNotInTx(table))
		return tx
	}

	if !tc.writable {
		tx.setErr(ErrTableNotWritableInTx(table))
		return tx
	}

	err := tc.table.put(key, value)
	if err != nil {
		tx.setErr(err)
	}

	return tx
}

// PrepareTx prepares a new database transaction.
//
// A prepared transaction may be reused multiple times, and is safe for
// concurrent access by multiple goroutines.
func (db *DB) PrepareTx(opts ...TxOption) (*TxConfig, error) {
	prepCfg := definePrepTxCfg(opts...)
	nbTables := len(prepCfg.readTables) + len(prepCfg.writeTables)
	cfg := TxConfig{
		db:        db,
		lockOrder: make([]*txTableCfg, 0, nbTables),
		tables:    make(map[TableKey]*txTableCfg, nbTables),
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	// Determine all tables involved and store it in the tx config object.
	for i, keys := range [][]TableKey{prepCfg.readTables, prepCfg.writeTables} {
		writable := i == 1

		for _, key := range keys {
			if _, ok := cfg.tables[key]; ok {
				return nil, fmt.Errorf("table %q locked twice", key)
			}

			tab, ok := db.tables[key]
			if !ok {
				return nil, fmt.Errorf("table %q does not exist", key)
			}
			lock, ok := db.locks[key]
			if !ok {
				return nil, fmt.Errorf("lock %q does not exist", key)
			}

			tc := &txTableCfg{
				key:      key,
				writable: writable,
				table:    tab,
				lock:     lock,
			}
			cfg.tables[key] = tc
			cfg.lockOrder = append(cfg.lockOrder, tc)
		}
	}

	// CRITICAL: sort all tables in a stable locking order. This prevents
	// deadlocks by ensuring that all tables are locked/unlocked in the
	// same order.
	sort.Slice(cfg.lockOrder, func(i, j int) bool {
		return cfg.lockOrder[i].key < cfg.lockOrder[j].key
	})

	return &cfg, nil
}
