package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"redis"
	"sort"
	"strconv"
	"strings"
	"time"
)

func help(ud interface{}, args []string) (result string, err error) {
	context := ud.(*Context)
	c := context.c

	for cmd := range c.handlers {
		result = result + cmd + "\n"
	}
	return
}

func shutdown(ud interface{}, args []string) (result string, err error) {
	passwd := ""
	if len(args) > 0 {
		passwd = args[0]
	}

	if passwd != "confirm" {
		err = errors.New("wrong password")
		return
	}

	context := ud.(*Context)
	context.c.Stop()
	context.m.Stop()
	context.s.Stop()
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

func sync_one(ud interface{}, args []string) (result string, err error) {
	context := ud.(*Context)
	sync_queue := context.sync_queue

	key := ""
	if len(args) > 0 {
		key = args[0]
	}
	sync_queue <- key
	return
}

func sync_all(ud interface{}, args []string) (result string, err error) {
	context := ud.(*Context)
	sync_queue := context.sync_queue

	cli := context.redis
	all_key_strings, err := cli.Exec("keys", "*")
	if err != nil {
		log.Printf("sync_all cmd service failed:%v", err)
		return
	}
	keys := all_key_strings.([]string)
	sort.Strings(keys)
	sz := len(keys)
	cur := 0
	for _, key := range keys {
		sync_queue <- key
		cur += 1
		if cur%100 == 0 {
			log.Printf("sync progress: %d/%d, queue:%d", cur, sz, len(sync_queue))
		}
	}
	log.Printf("sync finish: %d/%d", cur, sz)
	result = strconv.Itoa(sz)
	return
}

func count(ud interface{}, args []string) (result string, err error) {
	context := ud.(*Context)
	db := context.db
	it := db.NewIterator()
	defer it.Close()

	i := 0
	for it.SeekToFirst(); it.Valid(); it.Next() {
		i++
	}
	result = strconv.Itoa(i)
	return
}

func check(ud interface{}, args []string) (result string, err error) {
	context := ud.(*Context)
	db := context.db
	cli := context.redis
	it := db.NewIterator()
	count := 0
	mismatch_count := 0
	all_key_strings, err := cli.Exec("keys", "*")
	redis_key_count := len(all_key_strings.([]string))
	for it.SeekToFirst(); it.Valid(); it.Next() {
		var leveldb_data map[string]string
		if json_err := json.Unmarshal(it.Value(), &leveldb_data); json_err != nil {
			log.Printf("json unmarshal err:%v", json_err)
			log.Printf("it.Value():%v", it.Value())
		}
		redis_data := make(map[string]string)
		err_redis := cli.Hgetall(string(it.Key()), redis_data)
		if err_redis != nil {
			log.Printf("redis err:%v", err_redis)
		}
		if len(redis_data) != len(leveldb_data) {
			log.Printf("k/v amount mismatch:%v -> %d vs %d", string(it.Key()), len(redis_data), len(leveldb_data))
			mismatch_count++
			count++
			continue
		}
		for key, value := range redis_data {
			if value != leveldb_data[key] {
				log.Printf("key mismatch:%v", string(it.Key()))
				mismatch_count++
				break
			}
		}
		count++
	}
	result = fmt.Sprintf("%d counts, %d keys mismatch\n", count, mismatch_count)
	switch {
	case count > redis_key_count:
		result = result + fmt.Sprintf("redis key amount is less than leveldb:%d vs %d", redis_key_count, count)
	case count < redis_key_count:
		result = result + fmt.Sprintf("redis key amount is larger than leveldb:%d vs %d", redis_key_count, count)
	default:
		result = result + fmt.Sprintf("%d key compared, %d mismatch", count, mismatch_count)
	}

	return
}

func fast_check(ud interface{}, args []string) (result string, err error) {
	begin_timestamp := time.Now()
	context := ud.(*Context)
	db := context.db
	cli := context.redis
	it := db.NewIterator()
	defer it.Close()
	var leveldb_data map[string]string
	count := 0
	mismatch_count := 0
	all_key_strings, err := cli.Exec("keys", "*")
	redis_key_count := len(all_key_strings.([]string))
	for it.SeekToFirst(); it.Valid(); it.Next() {
		redis_version, err_redis := cli.Hget(string(it.Key()), "version")
		if err_redis != nil {
			log.Printf("redis err:%v", err_redis)
			return
		}
		if json_err := json.Unmarshal(it.Value(), &leveldb_data); json_err != nil {
			log.Printf("json unmarshal err:%v", json_err)
			log.Printf("it.Key:%v", string(it.Key()))
		}
		if redis_version != leveldb_data["version"] {
			mismatch_count++
			log.Printf("key mismatch:%s, redis_version:%v, leveldb_version:%s", string(it.Key()), redis_version, leveldb_data["version"])
		}
		count++
		if count%1000 == 0 {
			log.Printf("progress:%d/%d\n", count, redis_key_count)
		}
	}
	end_timestamp := time.Now()
	diff := end_timestamp.Sub(begin_timestamp)
	result = fmt.Sprintf("%d counts, %d keys mismatch in %f seconds\n", count, mismatch_count, diff.Seconds())
	switch {
	case count > redis_key_count:
		result = result + fmt.Sprintf("redis key amount is less than leveldb:%d vs %d", redis_key_count, count)
	case count < redis_key_count:
		result = result + fmt.Sprintf("redis key amount is larger than leveldb:%d vs %d", redis_key_count, count)
	default:
		result = result + fmt.Sprintf("%d key compared, %d mismatch", count, mismatch_count)
	}
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

	log.Printf("dump key:%s(%d)", key, len(chunk))
	var data map[string]string
	err = json.Unmarshal(chunk, &data)
	if err != nil {
		log.Printf("unmarshal chunk failed:%v", err)
		return
	}

	buf := bytes.NewBufferString("content:\n")
	for key, val := range data {
		fmt.Fprintf(buf, "%v:\t%v\n", key, val)
	}
	result = buf.String()
	return
}

func restore_one(ud interface{}, args []string) (result string, err error) {
	if len(args) < 1 {
		result = "restore need one argument\n"
		log.Println("restore need one argument")
		return
	}
	key := args[0]
	context := ud.(*Context)
	db := context.db
	chunk, err := db.Get([]byte(key))
	var leveldb_data map[string]string
	err = json.Unmarshal(chunk, &leveldb_data)
	cli := context.redis
	redis_data := make(map[string]string)
	err = cli.Hgetall(key, redis_data)
	if err != nil {
		log.Println(err)
		return
	}
	if redis_data["version"] >= leveldb_data["version"] {
		result = fmt.Sprintf("skip key:%s redis data version is the same with leveldb data", key)
		return
	}
	leveldb_array := make([]interface{}, len(leveldb_data)*2+1)
	leveldb_array[0] = key
	i := 1
	for k, v := range leveldb_data {
		leveldb_array[i] = k
		leveldb_array[i+1] = v
		i = i + 2
	}
	_, err = cli.Exec("hmset", leveldb_array...)
	if err != nil {
		log.Println(err)
		return
	}
	result = fmt.Sprintf("set key:%s", key)
	return
}

func restore_all(ud interface{}, args []string) (result string, err error) {
	context := ud.(*Context)
	db := context.db
	it := db.NewIterator()
	count := 0
	restore_count := 0
	for it.SeekToFirst(); it.Valid(); it.Next() {
		result, err = restore_one(ud, []string{string(it.Key())})
		if strings.HasPrefix(result, "set key") {
			restore_count++
		}
		count++
		if count%100 == 0 {
			log.Printf("progress:%d, restore:%d", count, restore_count)
		}
	}
	result = fmt.Sprintf("restore key %d, total %d\n", restore_count, count)
	return
}

func keys(ud interface{}, args []string) (result string, err error) {
	start := 0
	count := 10
	if len(args) > 0 {
		if start, err = strconv.Atoi(args[0]); err != nil {
			log.Println("iter start error:", err)
			return
		}
	}

	if len(args) > 1 {
		if count, err = strconv.Atoi(args[1]); err != nil {
			log.Println("iter start error:", err)
			return
		}
	}

	context := ud.(*Context)
	db := context.db
	it := db.NewIterator()
	defer it.Close()

	buf := bytes.NewBufferString("keys:\n")
	i := 0
	for it.SeekToFirst(); it.Valid(); it.Next() {
		if start <= i && i <= start+count {
			//log.Printf("key:%v", string(it.Key()))
			fmt.Fprintf(buf, "%s\n", string(it.Key()))
		}
		i++
	}
	result = buf.String()
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

	var right map[string]string
	err = json.Unmarshal(chunk, &right)
	if err != nil {
		log.Printf("unmarshal chunk failed:%v", err)
		return
	}

	buf := bytes.NewBufferString("left:redis, right:leveldb\n")
	buf_len := buf.Len()
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

	if buf_len == buf.Len() {
		fmt.Fprintf(buf, "perfect match\n")
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
	c.Register("help", context, help)
	c.Register("info", context, info)
	c.Register("sync", context, sync_one)
	c.Register("sync_all", context, sync_all)
	c.Register("dump", context, dump)
	c.Register("count", context, count)
	c.Register("diff", context, diff)
	c.Register("shutdown", context, shutdown)
	c.Register("keys", context, keys)
	c.Register("check", context, check)
	c.Register("fast_check", context, fast_check)
	c.Register("restore_one", context, restore_one)
	c.Register("restore_all", context, restore_all)
}

func NewContext() *Context {
	context := new(Context)
	cli := redis.NewRedis(setting.Redis.Host, setting.Redis.Password, setting.Redis.Db)
	context.redis = cli
	context.quit_chan = make(chan bool)
	return context
}
