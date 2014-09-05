package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"redis"
)

func shutdown(ud interface{}, args []string) (result string, err error) {
	context := ud.(*Context)
	context.m.Stop()
	context.s.Stop()
	context.c.Stop()
	context.quit_chan <- true
	result = "done"
	return
}

func info(ud interface{}, args []string) (result string, err error) {
	context := ud.(*Context)
	db := context.db

	key := ""
	if len(args) > 0 {
		key = args[0]
	}
	result = db.Info(key)
	return
}

func dump(ud interface{}, args []string) (result string, err error) {
	if len(args) == 0 {
		err = errors.New("no key")
		return
	}

	key := args[0]
	context := ud.(*Context)
	db := context.db

	chunk, err := db.Get([]byte(key))
	if chunk == nil || err != nil {
		log.Printf("fetch data failed:%v", err)
		return
	}

	data := make(map[string]string)
	err = json.Unmarshal(chunk, &data)
	if err != nil {
		log.Printf("unmarshal chunk failed:%v", err)
		return
	}

	buf := bytes.NewBufferString("content:\n")
	for k, v := range data {
		fmt.Fprintf(buf, " %s -> %s\n", k, v)
	}
	result = buf.String()
	return
}

func zinc_iter(ud interface{}, args []string) (result string, err error) {
	context := ud.(*Context)
	db := context.db
	it := db.NewIterator()
	it.SeekToFirst()
	if !it.Valid() {
		log.Printf("iterator should be valid")
	}
	defer it.Close()
	for it = it; it.Valid(); it.Next() {
		log.Printf("key:%v\n val:%v", string(it.Key()), string(it.Value()))
	}
	return
}

func zinc_read(ud interface{}, args []string) (result string, err error) {
	if len(args) == 0 {
		err = errors.New("no key")
		return "", err
	}
	context := ud.(*Context)
	key := args[0]
	db := context.db
	chunk, err := db.Get([]byte(key))
	if chunk == nil || err != nil {
		log.Printf("fetch data failed, key:%v, err:%v", key, err)
		return
	}
	var content []string
	err = json.Unmarshal(chunk, &content)
	if err != nil {
		log.Printf("unmarshal chunk failed:%v", err)
		return
	}
	result = fmt.Sprintf("%v", content)
	log.Printf("content:%v", content)
	return
}

func diff(ud interface{}, args []string) (result string, err error) {
	if len(args) == 0 {
		err = errors.New("no key")
		return
	}

	key := args[0]
	context := ud.(*Context)

	cli := context.redis
	db := context.db
	// query redis
	left := make(map[string]string)
	err = cli.Hgetall(key, left)
	if err != nil {
		return
	}

	chunk, err := db.Get([]byte(key))
	if chunk == nil || err != nil {
		log.Printf("fetch data failed:%v", err)
		return
	}

	right := make(map[string]string)
	err = json.Unmarshal(chunk, &right)
	if err != nil {
		log.Printf("unmarshal chunk failed:%v", err)
		return
	}

	buf := bytes.NewBufferString("left:redis, right:unqlite\n")
	for k, v1 := range left {
		if v2, ok := right[k]; ok {
			if v1 != v2 {
				fmt.Fprintf(buf, "%s < %s, %s\n", k, v1, v2)
			}
		} else {
			fmt.Fprintf(buf, "%s, only in left\n", k)
		}
	}

	for k, _ := range right {
		if _, ok := left[k]; !ok {
			fmt.Fprintf(buf, "%s, only in right\n", k)
		}
	}
	result = buf.String()
	return
}

func (context *Context) Register(c *CmdService) {
	err := context.redis.Connect()
	if err != nil {
		log.Panicf("register cmd service failed:%v", err)
	}

	log.Printf("register command service")
	c.Register("info", context, info)
	c.Register("dump", context, dump)
	c.Register("diff", context, diff)
	c.Register("shutdown", context, shutdown)
	c.Register("zinc_read", context, zinc_read)
	c.Register("zinc", context, zinc_iter)
}

func NewContext() *Context {
	context := new(Context)
	cli := redis.NewRedis(setting.Redis.Host, setting.Redis.Password, setting.Redis.Db)
	context.redis = cli
	context.quit_chan = make(chan bool)
	return context
}
