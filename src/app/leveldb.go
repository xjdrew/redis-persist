package main

import (
	"errors"
	"levigo"
)

type Leveldb struct {
	env      *levigo.Env
	options  *levigo.Options
	roptions *levigo.ReadOptions
	woptions *levigo.WriteOptions
	db       *levigo.DB
}

func (self *Leveldb) Open(dbname string) (err error) {
	if self.db != nil {
		return
	}

	self.db, err = levigo.Open(dbname, self.options)
	return
}

func (self *Leveldb) BatchPut(args ...[]byte) error {
	sz := len(args)
	if sz == 0 || sz%2 != 0 {
		return errors.New("illegal parameters")
	}

	batch := levigo.NewWriteBatch()
	defer batch.Close()

	for i := 0; i < sz-1; i++ {
		batch.Put(args[i], args[i+1])
	}
	return self.db.Write(self.woptions, batch)
}

func (self *Leveldb) Put(key, value []byte) error {
	return self.db.Put(self.woptions, key, value)
}

func (self *Leveldb) Get(key []byte) ([]byte, error) {
	return self.db.Get(self.roptions, key)
}

func (self *Leveldb) Info(key string) string {
	property := "leveldb." + key
	prop := self.db.PropertyValue(property)
	if prop == "" {
		return "valid key:\n\tnum-files-at-level<N>\n\tstats\n\tsstables\n"
	}
	return prop
}

func (self *Leveldb) Close() {
	if self.db != nil {
		self.db.Close()
	}

	if self.options != nil {
		self.options.Close()
	}

	if self.env != nil {
		self.env.Close()
	}
}

func (self *Leveldb) NewIterator() *levigo.Iterator {
	return self.db.NewIterator(self.roptions)
}

func NewLeveldb(name string) *Leveldb {
	options := levigo.NewOptions()

	// options.SetComparator(cmp)
	options.SetCreateIfMissing(true)
	options.SetErrorIfExists(false)

	// set env
	env := levigo.NewDefaultEnv()
	options.SetEnv(env)

	// set cache
	cache := levigo.NewLRUCache(16 << 20)
	options.SetCache(cache)

	options.SetInfoLog(nil)
	options.SetParanoidChecks(false)
	options.SetWriteBufferSize(128 << 20)
	options.SetMaxOpenFiles(2000)
	options.SetBlockSize(4 * 1024)
	options.SetBlockRestartInterval(16)
	options.SetCompression(levigo.SnappyCompression)

	// set filter
	filter := levigo.NewBloomFilter(10)
	options.SetFilterPolicy(filter)

	roptions := levigo.NewReadOptions()
	roptions.SetVerifyChecksums(false)
	roptions.SetFillCache(true)

	woptions := levigo.NewWriteOptions()
	// set sync false
	woptions.SetSync(false)

	db := &Leveldb{env,
		options,
		roptions,
		woptions,
		nil}
	if err := db.Open(name); err != nil {
		Panic("open db failed, err:%v", err)
	} else {
		Info("open db succeed, dbname:%v", name)
	}
	return db
}
