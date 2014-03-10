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
    qlen int
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

func (s *Storer) recover(key string, err error) {
    log.Printf("recv message failed, try to reconnect to redis:%v", err)
    s.reconnect()
    s.save(key)
}

func (s *Storer) save(key string) {
    name, err := s.cli.Type(key)
    if err != nil {
        s.recover(key, err)
        return
    }

    if name != "hash" {
        log.Printf("unexpected key type, key:%s, type:%s", key, name)
        return
    }

    obj := make(map[string] string)
    err = s.cli.Hgetall(key, obj)
    if err != nil {
        s.recover(key, err)
        return
    }
    
    chunk, _ := json.Marshal(obj)
    err = s.uql.Store([]byte(key), chunk)
    if err != nil { // seems bad, but still try to service
        log.Printf("save key:%s failed, err:%v", key, err)
    } else {
        err = s.uql.Commit()
    }

    if err != nil {
        log.Printf("commit key:%s failed, err:%v", key, err)
    } else {
        log.Printf("save key:%s, data len:%d", key, len(chunk))
    }
    return
}

func (s *Storer) Start(queue chan string) {
    err := s.cli.Connect()
    if err != nil {
        log.Fatalf("start Storer failed:%v", err)
    }
    
    log.Print("start storer succeed")

    for {
        key := <- queue
        s.save(key)
        qlen := len(queue)
        if qlen > s.qlen {
            log.Printf("queue grow, current length:%d", qlen)
        }
        s.qlen = qlen
    }
}

func NewStorer(cli *redis.Redis, uql *unqlitego.Database) *Storer {
    return &Storer{cli, uql, 0}
}

