package main

import (
	"encoding/json"
	"strconv"
	"sync"
	"time"

	"redis"
)

type Storer struct {
	cli *redis.Redis
	db  *Leveldb
}

func (s *Storer) reconnect() {
	times := 0
	for {
		wait := times
		times = times + 1

		if wait > 30 {
			wait = 30
		}
		Info("try to reconnect storer, times:%d, wait:%d", times, wait)
		time.Sleep(time.Duration(wait) * time.Second)

		err := s.cli.ReConnect()
		if err != nil {
			Error("reconnect storer failed:%v", err)
			continue
		} else {
			break
		}
	}
}

func (s *Storer) retry(key string, err error) {
	Error("recv message failed, try to reconnect to redis:%v", err)
	s.reconnect()
	s.save(key)
}

func (s *Storer) expire(key string, resp map[string]string) {
	value, ok := resp["expire"]
	if !ok {
		return
	}
	seconds, err := strconv.Atoi(value)
	if err != nil {
		return
	}
	if seconds > 0 {
		Info("expire key:%s, seconds:%d", key, seconds)
		s.cli.Exec("expire", key, seconds)
	}
}

func (s *Storer) save(key string) {
	name, err := s.cli.Type(key)
	if err != nil {
		s.retry(key, err)
		return
	}

	if name != "hash" {
		Error("unexpected key type, key:%s, type:%s", key, name)
		return
	}

	resp := make(map[string]string)
	err = s.cli.Hgetall(key, resp)
	if err != nil {
		s.retry(key, err)
		return
	}

	chunk, err := json.Marshal(resp)
	if err != nil {
		Error("marshal obj failed, key:%s, obj:%v, err:%v", key, resp, err)
		return
	}

	index_key := indexKey(key)
	version := []byte(resp["version"])
	err = s.db.BatchPut([]byte(index_key), version, []byte(key), chunk)
	if err != nil {
		Error("save key:%s failed, err:%v", key, err)
		return
	}

	// expire key
	s.expire(key, resp)

	Info("save key:%s, data len:%d", key, len(chunk))
	return
}

func (s *Storer) Start(queue chan string, wg *sync.WaitGroup) {
	defer wg.Done()

	err := s.cli.Connect()
	if err != nil {
		Panic("start Storer failed:%v", err)
	}

	Info("start storer succeed")

	for key := range queue {
		s.save(key)
	}
	Info("queue is closed, storer will exit")
}

func NewStorer(db *Leveldb) *Storer {
	cli := redis.NewRedis(setting.Redis.Host, setting.Redis.Password, setting.Redis.Db)
	return &Storer{cli, db}
}

type StorerMgr struct {
	instances []*Storer
	queues    []chan string
	wg        sync.WaitGroup
}

func _hash(str string) int {
	h := 0
	for _, c := range str {
		h += int(c)
	}
	return h
}

func (m *StorerMgr) Start(queue chan string) {
	m.wg.Add(1)
	defer m.wg.Done()

	for i, instance := range m.instances {
		m.wg.Add(1)
		go instance.Start(m.queues[i], &m.wg)
	}

	// dispatch msg
	max := len(m.queues)
	for key := range queue {
		i := _hash(key) % max
		m.queues[i] <- key
	}

	Info("queue is closed, all storer will exit")
	for _, queue := range m.queues {
		close(queue)
	}
}

func (m *StorerMgr) Stop() {
	m.wg.Wait()
}

func NewStorerMgr(db *Leveldb, numInstances int) *StorerMgr {
	m := new(StorerMgr)
	m.instances = make([]*Storer, numInstances)
	m.queues = make([]chan string, numInstances)
	for i := 0; i < numInstances; i++ {
		m.instances[i] = NewStorer(db)
		m.queues[i] = make(chan string, 256)
	}
	return m
}
