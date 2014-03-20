package main

import (
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

func (self *Leveldb) Put(key, value []byte) (err error){
    return self.db.Put(self.woptions, key, value)
}

func (self *Leveldb) Get(key []byte) ([]byte, error){
    return self.db.Get(self.roptions, key)
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

func NewLeveldb() *Leveldb{
    env := levigo.NewDefaultEnv()
	options := levigo.NewOptions()

	// options.SetComparator(cmp)
    options.SetCreateIfMissing(true)
	options.SetErrorIfExists(false)
	// options.SetCache(cache)
	options.SetEnv(env)
	options.SetInfoLog(nil)
	options.SetWriteBufferSize(8 << 20)
	options.SetParanoidChecks(true)
	options.SetMaxOpenFiles(2000)
	options.SetBlockSize(4 * 1024)
	options.SetBlockRestartInterval(8)
	options.SetCompression(levigo.SnappyCompression)

    roptions := levigo.NewReadOptions()
    roptions.SetVerifyChecksums(true)
    roptions.SetFillCache(false)

    woptions := levigo.NewWriteOptions()
    woptions.SetSync(true)
    return &Leveldb{env,
        options, 
        roptions,
        woptions,
        nil}
}

