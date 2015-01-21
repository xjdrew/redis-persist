package main

import (
	"fmt"
	"time"

	"redis"
)

type Monitor struct {
	cli                 *redis.Redis
	notification_config string
	event               string
	qlen                int
	quit_flag           bool
	quit_chan           chan int
}

func (m *Monitor) subscribe() error {
	config_key := "notify-keyspace-events"
	_, err := m.cli.Exec("config", "set", config_key, m.notification_config)
	if err != nil {
		return err
	}
	Info("config set %s = %s", config_key, m.notification_config)

	_, err = m.cli.Exec("subscribe", m.event)
	if err != nil {
		return err
	}

	Info("subscribe: %s", m.event)
	return nil
}

func (m *Monitor) reconnect() bool {
	times := 0
	for {
		if m.quit_flag {
			Error("close redis connection, monitor will exit")
			return false
		}

		wait := times
		times = times + 1

		if wait > 30 {
			wait = 30
		}
		Info("try to reconnect monitor, times:%d, wait:%d", times, wait)
		time.Sleep(time.Duration(wait) * time.Second)

		err := m.cli.ReConnect()
		if err != nil {
			Error("reconnect monitor failed:%v", err)
			continue
		}
		err = m.subscribe()
		if err != nil {
			Error("subscribe monitor failed:%v", err)
			continue
		} else {
			break
		}
	}
	return true
}

func (m *Monitor) Start(queue chan string) {
	err := m.cli.Connect()
	if err != nil {
		Panic("start monitor failed:%v", err)
	}
	err = m.subscribe()
	if err != nil {
		Panic("start monitor failed:%v", err)
	}
	Info("start monitor succeed")

	for {
		resp, err := m.cli.ReadResponse()
		if err != nil {
			Error("recv message failed, try to reconnect to redis:%v", err)
			if m.reconnect() {
				continue
			} else {
				close(queue)
				break
			}
		}
		if data, ok := resp.([]string); ok {
			if len(data) != 3 || data[0] != "message" {
				Error("receive unexpected message, %v", data)
			} else {
				event := data[1]
				key := data[2]
				Info("receive [%s], value[%s]", event, key)
				queue <- key

				qlen := len(queue)
				if qlen > m.qlen {
					Error("queue grow, current length:%d", qlen)
				}
				m.qlen = qlen
			}
		} else {
			Error("receive unexpected message, %v", resp)
		}
	}
	m.quit_chan <- 1
}

func (m *Monitor) Stop() {
	m.quit_flag = true
	if m.cli != nil {
		m.cli.Close()
	}
	<-m.quit_chan
}

func NewMonitor() *Monitor {
	cli := redis.NewRedis(setting.Redis.Host, setting.Redis.Password, setting.Redis.Db)
	notification_config := "gE"
	event := fmt.Sprintf("__keyevent@%d__:%s", setting.Redis.Db, setting.Redis.Event)
	return &Monitor{cli, notification_config, event, 0, false, make(chan int)}
}
