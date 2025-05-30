package simplewaldb

import (
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

// TxTable is a table obtained within the context of a transaction. Operations
// on the table are only valid while the transaction is active.
type TxTable struct {
	tab *table
	tx  *Tx
}

// Read a record from the table into the buffer. This reads at most len(buf)
// bytes from the entry, therefore the buffer should be sized appropriately.
func (tt *TxTable) Read(key Key, buf []byte) (int, error) {
	if tt.tx.done {
		return 0, ErrTxDone
	}

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

	return tt.tab.put(key, data)
}

// Count returns the number of items in the table.
func (tt *TxTable) Count() (int, error) {
	if tt.tx.done {
		return 0, ErrTxDone
	}

	return tt.tab.count(), nil
}

// Tx is an open transaction in the DB.
type Tx struct {
	done bool
	cfg  *TxConfig
}

// Read obtains a table for reading within the context of the transaction.
func (tx *Tx) Read(key TableKey) (TxTable, error) {
	if tx.done {
		return TxTable{}, ErrTxDone
	}
	tc, ok := tx.cfg.tables[key]
	if !ok {
		return TxTable{}, ErrTableNotInTx(key)
	}
	return TxTable{tab: tc.table, tx: tx}, nil
}

// Write obtains a table for reading and writing in the context of a
// transaction.
//
// This is only possible for tables that have been specified as writable when
// preparing the transaction (i.e. in the writeTables parameter of [PrepareTx]).
func (tx *Tx) Write(key TableKey) (TxTable, error) {
	if tx.done {
		return TxTable{}, ErrTxDone
	}
	tc, ok := tx.cfg.tables[key]
	if !ok {
		return TxTable{}, ErrTableNotInTx(key)
	}
	if !tc.writable {
		return TxTable{}, ErrTableNotWritableInTx(key)
	}
	return TxTable{tab: tc.table, tx: tx}, nil
}

// PrepareTx prepares a new database transaction.
//
// A prepared transaction may be reused multiple times, and is safe for
// concurrent access by multiple goroutines.
//
// readTables is the list of tables that will be locked for reading only.
//
// writeTables is the list of tables that will be locked for both reading and
// writing.
func (db *DB) PrepareTx(readTables []TableKey, writeTables []TableKey) (*TxConfig, error) {
	nbTables := len(readTables) + len(writeTables)
	cfg := TxConfig{
		db:        db,
		lockOrder: make([]*txTableCfg, 0, nbTables),
		tables:    make(map[TableKey]*txTableCfg, nbTables),
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	// Determine all tables involved and store it in the tx config object.
	for i, keys := range [][]TableKey{readTables, writeTables} {
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

// RunTx runs the given function as a transaction. It ends the transaction after
// f returns.
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
