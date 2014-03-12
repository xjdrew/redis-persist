package main

import (
    "log"
    "time"
    "encoding/json"

    "redis"
    "unqlitego"
)

type Storer struct {
    cli *redis.Redis
    uql *unqlitego.Database
    changes int
    cur_changes int
    quit_chan chan int
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

func (s *Storer) commit(diff int) {
    s.cur_changes = s.cur_changes + diff
    if s.cur_changes >= s.changes {
        err := s.uql.Commit()
        if err != nil {
            log.Panicf("commit failed, changes:%d, err:%v", s.cur_changes, err)
        } else {
            log.Printf("commit succeed, changes:%d", s.cur_changes)
        }
        s.cur_changes = 0
    }
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

    obj := make(map[string] string)
    err = s.cli.Hgetall(key, obj)
    if err != nil {
        s.retry(key, err)
        return
    }
    
    chunk, err := json.Marshal(obj)
    if err != nil {
        log.Printf("marshal obj failed, key:%s, obj:%v, err:%v", key, obj, err)
        return
    }
    
    err = s.uql.Store([]byte(key), chunk)
    if err != nil { // seems bad, but still try to service
        log.Panicf("save key:%s failed, err:%v", key, err)
    } 

    log.Printf("save key:%s, data len:%d", key, len(chunk))
    s.commit(1)
    return
}

func (s *Storer) Start(queue chan string) {
    err := s.cli.Connect()
    if err != nil {
        log.Panicf("start Storer failed:%v", err)
    }
    
    log.Print("start storer succeed")

    for {
        key,ok := <- queue
        if !ok {
            log.Print("queue is closed, storer will exit")
            s.commit(s.changes)
            break
        }
        s.save(key)
    }
    s.quit_chan <- 1
}

func (s *Storer) Stop() {
    <- s.quit_chan
}

func NewStorer(cli *redis.Redis, uql *unqlitego.Database, changes int) *Storer {
    return &Storer{cli, uql, changes, 0, make(chan int)}
}

