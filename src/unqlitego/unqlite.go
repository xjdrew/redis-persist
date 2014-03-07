package unqlitego

/*
#cgo linux CFLAGS: -DUNQLITE_ENABLE_THREADS=1 -Wno-unused-but-set-variable
#cgo darwin CFLAGS: -DUNQLITE_ENABLE_THREADS=1
#cgo windows CFLAGS: -DUNQLITE_ENABLE_THREADS=1
#include "./unqlite.h"
#include <stdlib.h>
*/
import "C"

import (
	"fmt"
	"runtime"
	"unsafe"
)

// UnQLiteError ... standard error for this module
type UnQLiteError int

func (e UnQLiteError) Error() string {
	s := errString[e]
	if s == "" {
		return fmt.Sprintf("errno %d", int(e))
	}
	return s
}

const (
	// UnQLiteNoMemErr ...
	UnQLiteNoMemErr UnQLiteError = UnQLiteError(C.UNQLITE_NOMEM)
)

var errString = map[UnQLiteError]string{
	C.UNQLITE_NOMEM: "Out of memory",
}

// Database ...
type Database struct {
	handle *C.unqlite
}

// Cursor ...
type Cursor struct {
	parent *Database
	handle *C.unqlite_kv_cursor
}

func init() {
	C.unqlite_lib_init()
	if !IsThreadSafe() {
		panic("unqlite library was not compiled for thread-safe option UNQLITE_ENABLE_THREADS=1")
	}
}

// NewDatabase ...
func NewDatabase(filename string) (db *Database, err error) {
	db = &Database{}
	name := C.CString(filename)
	defer C.free(unsafe.Pointer(name))
	res := C.unqlite_open(&db.handle, name, C.UNQLITE_OPEN_CREATE)
	if res != C.UNQLITE_OK {
		err = UnQLiteError(res)
	}
	if db.handle != nil {
		runtime.SetFinalizer(db, (*Database).Close)
	}
	return
}

// Close ...
func (db *Database) Close() (err error) {
	if db.handle != nil {
		res := C.unqlite_close(db.handle)
		if res != C.UNQLITE_OK {
			err = UnQLiteError(res)
		}
		db.handle = nil
	}
	return
}

// Store ...
func (db *Database) Store(key, value []byte) (err error) {
	res := C.unqlite_kv_store(db.handle,
		unsafe.Pointer(&key[0]), C.int(len(key)),
		unsafe.Pointer(&value[0]), C.unqlite_int64(len(value)))
	if res == C.UNQLITE_OK {
		return nil
	}
	return UnQLiteError(res)
}

// Append ...
func (db *Database) Append(key, value []byte) (err error) {
	res := C.unqlite_kv_append(db.handle,
		unsafe.Pointer(&key[0]), C.int(len(key)),
		unsafe.Pointer(&value[0]), C.unqlite_int64(len(value)))
	if res != C.UNQLITE_OK {
		err = UnQLiteError(res)
	}
	return
}

// Fetch ...
func (db *Database) Fetch(key []byte) (value []byte, err error) {
	var n C.unqlite_int64
	res := C.unqlite_kv_fetch(db.handle, unsafe.Pointer(&key[0]), C.int(len(key)), nil, &n)
	if res != C.UNQLITE_OK {
		err = UnQLiteError(res)
		return
	}
	value = make([]byte, int(n))
	res = C.unqlite_kv_fetch(db.handle, unsafe.Pointer(&key[0]), C.int(len(key)), unsafe.Pointer(&value[0]), &n)
	if res != C.UNQLITE_OK {
		err = UnQLiteError(res)
	}
	return
}

// Delete ...
func (db *Database) Delete(key []byte) (err error) {
	res := C.unqlite_kv_delete(db.handle, unsafe.Pointer(&key[0]), C.int(len(key)))
	if res != C.UNQLITE_OK {
		err = UnQLiteError(res)
	}
	return
}

// Begin ...
func (db *Database) Begin() (err error) {
	res := C.unqlite_begin(db.handle)
	if res != C.UNQLITE_OK {
		err = UnQLiteError(res)
	}
	return
}

// Commit ...
func (db *Database) Commit() (err error) {
	res := C.unqlite_commit(db.handle)
	if res != C.UNQLITE_OK {
		err = UnQLiteError(res)
	}
	return
}

// Rollback ...
func (db *Database) Rollback() (err error) {
	res := C.unqlite_rollback(db.handle)
	if res != C.UNQLITE_OK {
		err = UnQLiteError(res)
	}
	return
}

// NewCursor ...
func (db *Database) NewCursor() (cursor *Cursor, err error) {
	cursor = &Cursor{parent: db}
	res := C.unqlite_kv_cursor_init(db.handle, &cursor.handle)
	if res != C.UNQLITE_OK {
		err = UnQLiteError(res)
	}
	runtime.SetFinalizer(cursor, (*Cursor).Close)
	return
}

// Close ...
func (curs *Cursor) Close() (err error) {
	if curs.parent.handle != nil && curs.handle != nil {
		res := C.unqlite_kv_cursor_release(curs.parent.handle, curs.handle)
		if res != C.UNQLITE_OK {
			err = UnQLiteError(res)
		}
		curs.handle = nil
	}
	return
}

// Seek ...
func (curs *Cursor) Seek(key []byte) (err error) {
	res := C.unqlite_kv_cursor_seek(curs.handle, unsafe.Pointer(&key[0]), C.int(len(key)), C.UNQLITE_CURSOR_MATCH_EXACT)
	if res != C.UNQLITE_OK {
		err = UnQLiteError(res)
	}
	return
}

// SeekLE ...
func (curs *Cursor) SeekLE(key []byte) (err error) {
	res := C.unqlite_kv_cursor_seek(curs.handle, unsafe.Pointer(&key[0]), C.int(len(key)), C.UNQLITE_CURSOR_MATCH_LE)
	if res != C.UNQLITE_OK {
		err = UnQLiteError(res)
	}
	return
}

// SeekGE ...
func (curs *Cursor) SeekGE(key []byte) (err error) {
	res := C.unqlite_kv_cursor_seek(curs.handle, unsafe.Pointer(&key[0]), C.int(len(key)), C.UNQLITE_CURSOR_MATCH_GE)
	if res != C.UNQLITE_OK {
		err = UnQLiteError(res)
	}
	return
}

// First ...
func (curs *Cursor) First() (err error) {
	res := C.unqlite_kv_cursor_first_entry(curs.handle)
	if res != C.UNQLITE_OK {
		err = UnQLiteError(res)
	}
	return
}

// Last ...
func (curs *Cursor) Last() (err error) {
	res := C.unqlite_kv_cursor_last_entry(curs.handle)
	if res != C.UNQLITE_OK {
		err = UnQLiteError(res)
	}
	return
}

// IsValid ...
func (curs *Cursor) IsValid() (ok bool) {
	return C.unqlite_kv_cursor_valid_entry(curs.handle) == 1
}

// Next ...
func (curs *Cursor) Next() (err error) {
	res := C.unqlite_kv_cursor_next_entry(curs.handle)
	if res != C.UNQLITE_OK {
		err = UnQLiteError(res)
	}
	return
}

// Prev ...
func (curs *Cursor) Prev() (err error) {
	res := C.unqlite_kv_cursor_prev_entry(curs.handle)
	if res != C.UNQLITE_OK {
		err = UnQLiteError(res)
	}
	return
}

// Delete ...
func (curs *Cursor) Delete() (err error) {
	res := C.unqlite_kv_cursor_delete_entry(curs.handle)
	if res != C.UNQLITE_OK {
		err = UnQLiteError(res)
	}
	return
}

// Reset ...
func (curs *Cursor) Reset() (err error) {
	res := C.unqlite_kv_cursor_reset(curs.handle)
	if res != C.UNQLITE_OK {
		err = UnQLiteError(res)
	}
	return
}

// Key ...
func (curs *Cursor) Key() (key []byte, err error) {
	var n C.int
	res := C.unqlite_kv_cursor_key(curs.handle, nil, &n)
	if res != C.UNQLITE_OK {
		err = UnQLiteError(res)
		return
	}
	key = make([]byte, int(n))
	res = C.unqlite_kv_cursor_key(curs.handle, unsafe.Pointer(&key[0]), &n)
	if res != C.UNQLITE_OK {
		err = UnQLiteError(res)
	}
	return
}

// Value ...
func (curs *Cursor) Value() (value []byte, err error) {
	var n C.unqlite_int64
	res := C.unqlite_kv_cursor_data(curs.handle, nil, &n)
	if res != C.UNQLITE_OK {
		err = UnQLiteError(res)
		return
	}
	value = make([]byte, int(n))
	res = C.unqlite_kv_cursor_data(curs.handle, unsafe.Pointer(&value[0]), &n)
	if res != C.UNQLITE_OK {
		err = UnQLiteError(res)
	}
	return
}

// Shutdown ...
func Shutdown() (err error) {
	res := C.unqlite_lib_shutdown()
	if res != C.UNQLITE_OK {
		err = UnQLiteError(res)
	}
	return
}

// IsThreadSafe ...
func IsThreadSafe() bool {
	return C.unqlite_lib_is_threadsafe() == 1
}

// Version ...
func Version() string {
	return C.GoString(C.unqlite_lib_version())
}

// Signature ...
func Signature() string {
	return C.GoString(C.unqlite_lib_signature())
}

// Ident ...
func Ident() string {
	return C.GoString(C.unqlite_lib_ident())
}

// Copyright ...
func Copyright() string {
	return C.GoString(C.unqlite_lib_copyright())
}

/* TODO: implement

// Database Engine Handle
int unqlite_config(unqlite *pDb,int nOp,...);

// Key/Value (KV) Store Interfaces
int unqlite_kv_fetch_callback(unqlite *pDb,const void *pKey,
	                    int nKeyLen,int (*xConsumer)(const void *,unsigned int,void *),void *pUserData);
int unqlite_kv_config(unqlite *pDb,int iOp,...);

//  Cursor Iterator Interfaces
int unqlite_kv_cursor_key_callback(unqlite_kv_cursor *pCursor,int (*xConsumer)(const void *,unsigned int,void *),void *pUserData);
int unqlite_kv_cursor_data_callback(unqlite_kv_cursor *pCursor,int (*xConsumer)(const void *,unsigned int,void *),void *pUserData);

// Utility interfaces
int unqlite_util_load_mmaped_file(const char *zFile,void **ppMap,unqlite_int64 *pFileSize);
int unqlite_util_release_mmaped_file(void *pMap,unqlite_int64 iFileSize);

// Global Library Management Interfaces
int unqlite_lib_config(int nConfigOp,...);
*/
