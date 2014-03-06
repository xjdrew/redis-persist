// Package gounqlite provides access to the UnQLite C API, version 1.1.6.
package gounqlite

/*
#cgo linux CFLAGS: -DUNQLITE_ENABLE_THREADS=1 -Wno-unused-but-set-variable -I.
#cgo darwin CFLAGS: -DUNQLITE_ENABLE_THREADS=1
#include "unqlite.h"
#include <stdlib.h>
*/
import "C"

import (
	"errors"
	"fmt"
	"unsafe"
)

// Errno is an UnQLite library error number.
type Errno int

func (e Errno) Error() string {
	s := errText[e]
	if s == "" {
		return fmt.Sprintf("errno %d", int(e))
	}
	return s
}

var (
	ErrLock     error = Errno(-76) // /* Locking protocol error */
	ErrReadOnly error = Errno(-75) // /* Read only Key/Value storage engine */
	ErrOpen     error = Errno(-74) // /* Unable to open the database file */
	ErrFull     error = Errno(-73) // /* Full database (unlikely) */
	ErrVM       error = Errno(-71) // /* Virtual machine error */
	ErrCompile  error = Errno(-70) // /* Compilation error */
	Done              = Errno(-28) // Not an error. /* Operation done */
	ErrCorrupt  error = Errno(-24) // /* Corrupt pointer */
	ErrNoOp     error = Errno(-20) // /* No such method */
	ErrPerm     error = Errno(-19) // /* Permission error */
	ErrEOF      error = Errno(-18) // /* End Of Input */
	ErrNotImpl  error = Errno(-17) // /* Method not implemented by the underlying Key/Value storage engine */
	ErrBusy     error = Errno(-14) // /* The database file is locked */
	ErrUnknown  error = Errno(-13) // /* Unknown configuration option */
	ErrExists   error = Errno(-11) // /* Record exists */
	ErrAbort    error = Errno(-10) // /* Another thread have released this instance */
	ErrInvalid  error = Errno(-9)  // /* Invalid parameter */
	ErrLimit    error = Errno(-7)  // /* Database limit reached */
	ErrNotFound error = Errno(-6)  // /* No such record */
	ErrLocked   error = Errno(-4)  // /* Forbidden Operation */
	ErrEmpty    error = Errno(-3)  // /* Empty record */
	ErrIO       error = Errno(-2)  // /* IO error */
	ErrNoMem    error = Errno(-1)  // /* Out of memory */
)

var errText = map[Errno]string{
	-76: "Locking protocol error",
	-75: "Read only Key/Value storage engine",
	-74: "Unable to open the database file",
	-73: "Full database",
	-71: "Virtual machine error",
	-70: "Compilation error",
	-28: "Operation done", // Not an error.
	-24: "Corrupt pointer",
	-20: "No such method",
	-19: "Permission error",
	-18: "End Of Input",
	-17: "Method not implemented by the underlying Key/Value storage engine",
	-14: "The database file is locked",
	-13: "Unknown configuration option",
	-11: "Record exists",
	-10: "Another thread have released this instance",
	-9:  "Invalid parameter",
	-7:  "Database limit reached",
	-6:  "No such record",
	-4:  "Forbidden Operation",
	-3:  "Empty record",
	-2:  "IO error",
	-1:  "Out of memory",
}

// Handle is a UnQLite database engine handle.
type Handle struct {
	db *C.unqlite
}

// init initializes the UnQlite library.
func init() {
	C.unqlite_lib_init()
}

// Open creates a database handle.
func Open(filename string) (*Handle, error) {
	if !Threadsafe() {
		return nil, errors.New("unqlite library was not compiled for thread-safe operation")
	}
	var db *C.unqlite
	name := C.CString(filename)
	defer C.free(unsafe.Pointer(name))
	rv := C.unqlite_open(&db, name, C.UNQLITE_OPEN_CREATE)
	if rv != C.UNQLITE_OK {
		return nil, Errno(rv)
	}
	if db == nil {
		return nil, errors.New("unqlite unable to allocate memory to hold the database")
	}
	return &Handle{db}, nil
}

// Close closes the database handle.
func (h *Handle) Close() error {
	if h == nil || h.db == nil {
		return errors.New("nil unqlite database")
	}
	rv := C.unqlite_close(h.db)
	if rv != C.UNQLITE_OK {
		return Errno(rv)
	}
	h.db = nil
	return nil
}

// Store inserts or updates a key-value pair in the database.
//
// If the key exists, its value is updated. Otherwise, a new key-value pair is created.
func (h *Handle) Store(key, value []byte) error {
	rv := C.unqlite_kv_store(h.db, unsafe.Pointer(&key[0]), C.int(len(key)), unsafe.Pointer(&value[0]), C.unqlite_int64(len(value)))
	if rv == C.UNQLITE_OK {
		return nil
	}
	return Errno(rv)
}

// Append appends value to the key-value pair in the database.
func (h *Handle) Append(key, value []byte) error {
	rv := C.unqlite_kv_append(h.db, unsafe.Pointer(&key[0]), C.int(len(key)), unsafe.Pointer(&value[0]), C.unqlite_int64(len(value)))
	if rv == C.UNQLITE_OK {
		return nil
	}
	return Errno(rv)
}

// Fetch retrieves the value for the specified key from the database.
func (h *Handle) Fetch(key []byte) (value []byte, err error) {
	// Fetch size of value.
	var n C.unqlite_int64
	rv := C.unqlite_kv_fetch(h.db, unsafe.Pointer(&key[0]), C.int(len(key)), nil, &n)
	if rv != C.UNQLITE_OK {
		return nil, Errno(rv)
	}
	// Fetch value.
	b := make([]byte, int(n))
	rv = C.unqlite_kv_fetch(h.db, unsafe.Pointer(&key[0]), C.int(len(key)), unsafe.Pointer(&b[0]), &n)
	if rv != C.UNQLITE_OK {
		return nil, Errno(rv)
	}
	return b, nil
}

// Delete removes a key-value pair from the database.
func (h *Handle) Delete(key []byte) error {
	rv := C.unqlite_kv_delete(h.db, unsafe.Pointer(&key[0]), C.int(len(key)))
	if rv != C.UNQLITE_OK {
		return Errno(rv)
	}
	return nil
}

// Commit commits all changes to the database.
func (h *Handle) Commit() error {
	rv := C.unqlite_commit(h.db)
	if rv != C.UNQLITE_OK {
		return Errno(rv)
	}
	return nil
}

// Threadsafe returns true if the UnQlite library was compiled for thread-safe operation, else false.
func Threadsafe() bool {
	return C.unqlite_lib_is_threadsafe() == 1
}

// Version returns the UnQLite library version.
func Version() string {
	p := C.unqlite_lib_version()
	return C.GoString(p)
}

// Signature returns the UnQLite library signature.
func Signature() string {
	p := C.unqlite_lib_signature()
	return C.GoString(p)
}

// Ident returns the UnQLite library revision identifier.
func Ident() string {
	p := C.unqlite_lib_ident()
	return C.GoString(p)
}

// Copyright returns the UnQLite library copyright notice.
func Copyright() string {
	p := C.unqlite_lib_copyright()
	return C.GoString(p)
}
