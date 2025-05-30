package simplewaldb

import (
	"errors"
	"fmt"
	"os"
	"sync"
)

// DB is the main database object.
type DB struct {
	mu     sync.Mutex
	closed bool

	locks  map[TableKey]*sync.RWMutex
	tables map[TableKey]*table
}

// NewDB creates or opens a new DB.
func NewDB(opts ...Option) (*DB, error) {
	cfg := defineOptions(opts...)

	if stat, err := os.Stat(cfg.rootDir); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	} else if err != nil {
		// err == ErrNotExist
		err := os.MkdirAll(cfg.rootDir, 0o700)
		if err != nil {
			return nil, err
		}
	} else if !stat.IsDir() {
		return nil, fmt.Errorf("root dir %q is not a dir", cfg.rootDir)
	}

	db := &DB{
		locks:  make(map[TableKey]*sync.RWMutex, len(cfg.tables)),
		tables: make(map[TableKey]*table, len(cfg.tables)),
	}

	// Init tables.
	var tables []*table
	for _, tableKey := range cfg.tables {
		tab, err := newTable(cfg.rootDir, tableKey, cfg.separator)
		if err != nil {
			// Close previous tables.
			for _, tab := range tables {
				// Ignore error because we're leaving already.
				_ = tab.close()
			}
			return nil, err
		}
		tables = append(tables, tab)
		db.tables[tableKey] = tab
		db.locks[tableKey] = new(sync.RWMutex)
	}

	return db, nil
}

// Close the DB. It cannot be used after this returns.
//
// This function is NOT safe for concurrent calls with other DB operations.
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return errors.New("already closed")
	}
	db.closed = true

	var firstErr error
	for _, tab := range db.tables {
		err := tab.close()
		if firstErr != nil {
			firstErr = err
		}
	}

	return firstErr
}

// BeginTx begins a new prepared transaction.
//
// EndTx MUST be called, otherwise this may deadlock the database.
func (db *DB) BeginTx(cfg *TxConfig) (Tx, error) {
	// Acquire all locks.
	tx := Tx{cfg: cfg}
	// log.Printf("%p locking %v", tx.cfg, len(cfg.tables))
	for _, tc := range cfg.lockOrder {
		// log.Printf("%p locking   %s %v", tx.cfg, tc.key, tc.writable)
		if tc.writable {
			tc.lock.Lock()
		} else {
			tc.lock.RLock()
		}
	}
	// log.Printf("%p locked  %v", tx.cfg, len(cfg.tables))

	return tx, nil
}

// EndTx finishes the transaction and releases all table locks.
//
// This MUST be called, otherwise the database may deadlock.
func (db *DB) EndTx(tx *Tx) error {
	if tx.done {
		return fmt.Errorf("transaction was already done")
	}

	// Release all locks in reverse order.
	// log.Printf("%p releas  %v", tx.cfg, len(tx.cfg.tables))
	for i := len(tx.cfg.lockOrder) - 1; i >= 0; i-- {
		tc := tx.cfg.lockOrder[i]
		// log.Printf("%p unlocking %s %v", tx.cfg, tc.key, tc.writable)
		if tc.writable {
			tc.lock.Unlock()
		} else {
			tc.lock.RUnlock()
		}
	}
	// log.Printf("%p done    %v", tx.cfg, len(tx.cfg.tables))
	tx.done = true
	return nil
}
