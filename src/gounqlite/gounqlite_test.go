package gounqlite

import (
	"bytes"
	"testing"
)

func TestErrnoError(t *testing.T) {
	for k, v := range errText {
		if s := k.Error(); s != v {
			t.Errorf("Errno(%d).Error() = %s, expected %s", k, s, v)
		}
	}
}

func TestOpenCloseInMemoryDatabase(t *testing.T) {
	db, err := Open(":mem:")
	if err != nil {
		t.Fatalf("Open(\":mem:\") error: %s", err)
	}
	err = db.Close()
	if err != nil {
		t.Fatalf("db.Close() error: %s", err)
	}
}

type keyValuePair struct {
	key   []byte
	value []byte
}

var keyValuePairs = []keyValuePair{
	{[]byte("key"), []byte("value")},
	{[]byte("hello"), []byte("世界")},
	{[]byte("hello"), []byte("world")},
	{[]byte("gordon"), []byte("gopher")},
	{[]byte{'f', 'o', 'o'}, []byte{'b', 'a', 'r'}},
	{[]byte{'\000'}, []byte{42}},
}

func TestStore(t *testing.T) {
	db, err := Open(":mem:")
	if err != nil {
		t.Fatalf("Open(\":mem:\") error: %s", err)
	}
	defer db.Close()

	for _, p := range keyValuePairs {
		if err := db.Store(p.key, p.value); err != nil {
			t.Fatalf("db.Store(%v, %v) error: %v", p.key, p.value, err)
		}
		b, err := db.Fetch(p.key)
		if err != nil {
			t.Fatalf("db.Fetch(%v) error: %v", p.key, err)
		}
		if !bytes.Equal(b, p.value) {
			t.Errorf("db.Fetch(%v) = %v, expected %v", p.key, b, p.value)
		}
	}
}

func TestAppend(t *testing.T) {
	db, err := Open(":mem:")
	if err != nil {
		t.Fatalf("Open(\":mem:\") error: %s", err)
	}
	defer db.Close()
	p := keyValuePairs[0]
	if err := db.Store(p.key, p.value); err != nil {
		t.Fatalf("db.Store(%v, %v) error: %v", p.key, p.value, err)
	}
	b, err := db.Fetch(p.key)
	if err != nil {
		t.Fatalf("db.Fetch(%v) error: %v", p.key, err)
	}
	if !bytes.Equal(b, p.value) {
		t.Errorf("db.Fetch(%v) = %v, expected %v", p.key, b, p.value)
	}
	if err := db.Append(p.key, p.value); err != nil {
		t.Fatalf("db.Append(%v, %v) error: %v", p.key, p.value, err)
	}
	bb, err := db.Fetch(p.key)
	if err != nil {
		t.Fatalf("db.Fetch(%v) error: %v", p.key, err)
	}
	if !bytes.Equal(bb, append(b, b...)) {
		t.Errorf("db.Fetch(%v) = %v, expected %v", p.key, bb, append(b, b...))
	}
}

func TestDelete(t *testing.T) {
	db, err := Open(":mem:")
	if err != nil {
		t.Fatalf("Open(\":mem:\") error: %s", err)
	}
	defer db.Close()

	for _, p := range keyValuePairs {
		// Test delete non-existing records.
		if err := db.Delete(p.key); err != ErrNotFound {
			t.Fatalf("db.Delete(%v) = %v, expected %v", p.key, err, ErrNotFound)
		}
		// Store records.
		if err := db.Store(p.key, p.value); err != nil {
			t.Fatalf("db.Store(%v, %v) error: %v", p.key, p.value, err)
		}
		// Fetch and compare records to make sure that they were stored.
		b, err := db.Fetch(p.key)
		if err != nil {
			t.Fatalf("db.Fetch(%v) error: %v", p.key, err)
		}
		if !bytes.Equal(b, p.value) {
			t.Errorf("db.Fetch(%v) = %v, expected %v", p.key, b, p.value)
		}
		// Test delete records.
		if err := db.Delete(p.key); err != nil {
			t.Fatalf("db.Delete(%v) = %v, expected %v", p.key, err, nil)
		}
	}
}

func TestCommit(t *testing.T) {
	db, err := Open(":mem:")
	if err != nil {
		t.Fatalf("Open(\":mem:\") error: %s", err)
	}
	defer db.Close()

	for _, p := range keyValuePairs {
		if err := db.Store(p.key, p.value); err != nil {
			t.Fatalf("db.Store(%v, %v) error: %v", p.key, p.value, err)
		}
		if err := db.Commit(); err != nil {
			t.Fatalf("db.Commit() error: %v", err)
		}
	}
}
