package main

import (
	"encoding/json"
	"log"
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
		log.Printf("try to reconnect storer, times:%d, wait:%d", times, wait)
		time.Sleep(time.Duration(wait) * time.Second)

		err := s.cli.ReConnect()
		if err != nil {
			log.Printf("reconnect storer failed:%v", err)
			continue
		} else {
			break
		}
	}
}

func (s *Storer) retry(key string, err error) {
	log.Printf("recv message failed, try to reconnect to redis:%v", err)
	s.reconnect()
	s.save(key)
}

func (s *Storer) save(key string) {
	name, err := s.cli.Type(key)
	if err != nil {
		s.retry(key, err)
		return
	}

	if name != "hash" {
		log.Printf("unexpected key type, key:%s, type:%s", key, name)
		return
	}

	resp, err := s.cli.Hgetall_arr(key)
	if err != nil {
		s.retry(key, err)
		return
	}

	chunk, err := json.Marshal(resp)
	if err != nil {
		log.Printf("marshal obj failed, key:%s, obj:%v, err:%v", key, resp, err)
		return
	}

	err = s.db.Put([]byte(key), chunk)
	if err != nil {
		log.Printf("save key:%s failed, err:%v", key, err)
		return
	}

	log.Printf("save key:%s, data len:%d", key, len(chunk))
	return
}

func (s *Storer) Start(queue chan string, wg sync.WaitGroup) {
	err := s.cli.Connect()
	if err != nil {
		log.Panicf("start Storer failed:%v", err)
	}

	log.Print("start storer succeed")

	for key := range queue {
		s.save(key)
	}
	log.Print("queue is closed, storer will exit")
	wg.Done()
}

func NewStorer(db *Leveldb) *Storer {
	cli := redis.NewRedis(setting.Redis.Host, setting.Redis.Password, setting.Redis.Db)
	return &Storer{cli, db}
}

type StorerMgr struct {
	instances []*Storer
	wg        sync.WaitGroup
}

func (m *StorerMgr) Start(queue chan string) {
	for _, instance := range m.instances {
		m.wg.Add(1)
		go instance.Start(queue, m.wg)
	}
}

func (m *StorerMgr) Stop() {
	m.wg.Wait()
}

func NewStorerMgr(db *Leveldb, numInstances int) *StorerMgr {
	m := new(StorerMgr)
	m.instances = make([]*Storer, numInstances)
	for i := 0; i < numInstances; i++ {
		m.instances[i] = NewStorer(db)
	}
	return m
}
