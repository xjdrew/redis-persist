package main

import (
    "log"
    "time"
    "encoding/json"

    "redis"
    "gounqlite"
)

type Storer struct {
    cli *redis.Redis
    uql *gounqlite.Handle
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
        resp, err := s.cli.Exec("type", key)
        if err != nil {
            s.recover(key, err)
            return
        }

        var data string
        var ok bool
        if data, ok = resp.(string); !ok {
            log.Printf("unexpected key type, key:%s, type:%v", key, resp)
            return
        }

        if data != "hash" {
            log.Printf("unexpected key type, key:%s, type:%s", key, data)
            return
        }

        resp, err = s.cli.Exec("hgetall", key)
        if err != nil {
            s.recover(key, err)
            return
        }
        
        if values, ok := resp.([]string); ok {
            sz := len(values)
            
            // if sz 不为整数，丢弃最后一项
            obj := make(map[string] string)
            for i:=0;i<sz-1;i=i+2 {
                obj[values[i]] = values[i+1]
            }
            chunk, _ := json.Marshal(obj)
            err = s.uql.Store([]byte(key), chunk)
            if err != nil { // seems bad, but still try to service
                log.Printf("save key:%s failed, err:%v", key, err)
            } else {
                log.Printf("save key:%s", key)
            }
        }
}

func (s *Storer) Start(queue chan string) {
    err := s.cli.Connect()
    if err != nil {
        log.Fatalf("start Storer failed:%v", err)
    }

    for {
        key := <- queue
        s.save(key)
    }
}

func NewStorer(cli *redis.Redis, uql *gounqlite.Handle ) *Storer {
    return &Storer{cli, uql}
}

