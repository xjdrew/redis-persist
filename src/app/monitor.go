package main

import (
    "log"
    "time"

    "redis"
)

type Monitor struct{
    cli *redis.Redis
    events string
    channel string
    qlen int
}

func (m *Monitor) subscribe() error {
    _, err := m.cli.Exec("config", "set", "notify-keyspace-events", m.events)
    if err != nil {
        return err
    }
    
    _, err = m.cli.Exec("subscribe", m.channel)
    if err != nil {
        return err
    }
    return nil
}

func (m *Monitor) reconnect() {
    times := 0
    for {
        wait := times
        times = times + 1

        if wait > 30 {
            wait = 30
        }
        log.Printf("try to reconnect monitor, times:%d, wait:%d", times, wait)
        time.Sleep(time.Duration(wait) * time.Second)

        err := m.cli.ReConnect()
        if err != nil {
            log.Printf("reconnect monitor failed:%v", err)
            continue
        }
        
        err = m.subscribe()
        if err != nil {
            log.Printf("subscribe monitor failed:%v", err)
            continue
        } else {
            break
        }
    }
}

func (m *Monitor) Start(queue chan string) {
    err := m.cli.Connect()
    if err != nil {
        log.Fatalf("start monitor failed:%v", err)
    }
    err = m.subscribe()
    if err != nil {
        log.Fatalf("start monitor failed:%v", err)
    }
    
    log.Print("start monitor succeed")

    for {
        resp, err := m.cli.ReadResponse()
        if err != nil {
            log.Printf("recv message failed, try to reconnect to redis:%v", err)
            m.reconnect()
            continue
        }
        
        if data, ok := resp.([]string); ok {
            if len(data) != 3 || data[0] != "message" {
                log.Printf("receive unexpected message, %v", data)
            } else {
                event := data[1]
                key := data[2]
                log.Printf("receive [%s], value[%s]", event, key)
                queue <- key

                qlen := len(queue)
                if qlen > m.qlen {
                    log.Printf("queue grow, current length:%d", qlen)
                }
                m.qlen = qlen
            }
        } else {
            log.Printf("receive unexpected message, %v", resp)
        }
    }
}

func NewMonitor(cli *redis.Redis, events string, channel string) *Monitor {
    return &Monitor{cli, events, channel, 0}
}

