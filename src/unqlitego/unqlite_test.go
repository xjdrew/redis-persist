package unqlitego

import (
	"bytes"
	"fmt"
	. "github.com/r7kamura/gospel"
	"io/ioutil"
	"testing"
)

func Testライブラリ(t *testing.T) {
	Describe(t, "正常系", func() {
		Context("基本テスト", func() {
			It("IsThreadSafe", func() {
				Expect(IsThreadSafe()).To(Equal, true)
			})
			It("Version", func() {
				Expect(Version()).To(Equal, "1.1.6")
			})
			It("Signature", func() {
				Expect(Signature()).To(Equal, "unqlite/1.1.6")
			})
			It("Ident", func() {
				Expect(Ident()).To(Equal, "unqlite:b172a1e2c3f62fb35c8e1fb2795121f82356cad6")
			})
			It("Copyright", func() {
				Expect(Copyright()).To(Equal, "Copyright (C) Symisc Systems, S.U.A.R.L [Mrad Chems Eddine <chm@symisc.net>] 2012-2013, http://unqlite.org/")
			})
		})
	})
}

func Testモジュール(t *testing.T) {
	var db *Database
	var src []byte
	src = []byte("value")

	Describe(t, "正常系", func() {
		Context("基本テスト", func() {
			It("NewDatabase", func() {
				f, err := ioutil.TempFile("", "sample.db")
				if err != nil {
					panic(err)
				}
				db, err = NewDatabase(f.Name())
				Expect(err).To(NotExist)
				Expect(db).To(Exist)
			})
			It("Database.Begin", func() {
				err := db.Begin()
				Expect(err).To(NotExist)
			})
			It("Database.Store", func() {
				err := db.Store([]byte("sample"), src)
				Expect(err).To(NotExist)
			})
			It("Database.Fetch", func() {
				dst, err := db.Fetch([]byte("sample"))
				Expect(err).To(NotExist)
				Expect(bytes.Compare(src, dst)).To(Equal, 0)
			})
			It("Database.Append", func() {
				err1 := db.Append([]byte("sample"), []byte(" append"))
				Expect(err1).To(NotExist)
				dst, err2 := db.Fetch([]byte("sample"))
				Expect(err2).To(NotExist)
				Expect(bytes.Compare(append(src, []byte(" append")...), dst)).To(Equal, 0)
			})
			It("Database.Commit", func() {
				err := db.Commit()
				Expect(err).To(NotExist)
			})
			It("Database.Begin", func() {
				err := db.Begin()
				Expect(err).To(NotExist)
			})
			It("Database.Delete", func() {
				err1 := db.Delete([]byte("sample"))
				Expect(err1).To(NotExist)
				_, err2 := db.Fetch([]byte("sample"))
				Expect(err2).To(Exist)
			})
			It("Database.Rollback", func() {
				err := db.Rollback()
				Expect(err).To(NotExist)
				value, err2 := db.Fetch([]byte("sample"))
				Expect(err2).To(NotExist)
				Expect(value).To(Exist)
			})
			It("Database.NewCursor", func() {
				cursor, err := db.NewCursor()
				Expect(err).To(NotExist)
				Expect(cursor).To(Exist)
				err = cursor.Seek([]byte("sample"))
				Expect(err).To(NotExist)
			})
			It("Database.Close", func() {
				err := db.Close()
				Expect(err).To(NotExist)
			})
		})
	})
}

func BenchmarkFileStore(b *testing.B) {
	b.StopTimer()
	f, err := ioutil.TempFile("", "sample.db")
	if err != nil {
		panic(err)
	}
	db, _ := NewDatabase(f.Name())
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		db.Store([]byte(fmt.Sprintf("%d", i)), []byte("abcdefghijklmnopabcdefghijklmnopabcdefghijklmnopabcdefghijklmnop"))
	}
}

func BenchmarkFileFetch(b *testing.B) {
	b.StopTimer()
	f, err := ioutil.TempFile("", "sample.db")
	if err != nil {
		panic(err)
	}
	db, _ := NewDatabase(f.Name())
	for i := 0; i < b.N; i++ {
		db.Store([]byte(fmt.Sprintf("%d", i)), []byte("abcdefghijklmnopabcdefghijklmnopabcdefghijklmnopabcdefghijklmnop"))
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		_, _ = db.Fetch([]byte(fmt.Sprintf("%d", i)))
	}
}

func BenchmarkMemStore(b *testing.B) {
	b.StopTimer()
	db, _ := NewDatabase("")
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		db.Store([]byte(fmt.Sprintf("%d", i)), []byte("abcdefghijklmnopabcdefghijklmnopabcdefghijklmnopabcdefghijklmnop"))
	}
}

func BenchmarkMemFetch(b *testing.B) {
	b.StopTimer()
	db, _ := NewDatabase("")
	for i := 0; i < b.N; i++ {
		db.Store([]byte(fmt.Sprintf("%d", i)), []byte("abcdefghijklmnopabcdefghijklmnopabcdefghijklmnopabcdefghijklmnop"))
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		_, _ = db.Fetch([]byte(fmt.Sprintf("%d", i)))
	}
}
