package simplewaldb

import (
	"errors"
	"fmt"
)

// ErrTxDone is returned when a transaction has already completed.
var ErrTxDone = errors.New("transaction is done")

// ErrTableNotInTx is returned when a table does not exist in the database.
type ErrTableNotInTx TableKey

func (err ErrTableNotInTx) Error() string {
	return fmt.Sprintf("table %q not bound to tx", string(err))
}

func (err ErrTableNotInTx) Is(target error) bool {
	_, ok := target.(ErrKeyNotFound)
	return ok
}

// ErrTableNotWritableInTx is returned when the table is not writable in the
// transaction.
type ErrTableNotWritableInTx TableKey

func (err ErrTableNotWritableInTx) Error() string {
	return fmt.Sprintf("table %q not writable in tx", string(err))
}

// ErrKeyNotFound is returned when a key is not found.
type ErrKeyNotFound Key

func (err ErrKeyNotFound) Error() string {
	return fmt.Sprintf("key %x not found", err[:])
}

func (err ErrKeyNotFound) Is(target error) bool {
	_, ok := target.(ErrKeyNotFound)
	return ok
}
