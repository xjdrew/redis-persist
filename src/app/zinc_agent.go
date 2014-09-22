package main

import (
	"custom_jsonrpc"
	"encoding/json"
	"net"
	"net/rpc"
)

type ZincAgent struct {
	listener net.Listener
	addr     string
	db       *Leveldb
}

func StartZincAgent(agent *ZincAgent) {
	rpc.Register(agent)
	tcpAddr, err := net.ResolveTCPAddr("tcp", agent.addr)
	if err != nil {
		Panic("Error: %v", err)
	}
	listener, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		Panic("Error: %v", err)
	}

	Info("Start json rpc on %v", tcpAddr)
	for {
		conn, err := listener.Accept()
		if err != nil {
			Error("accept conn failed:%v", err)
			continue
		}
		Info("New conn:%v", conn)
		go custom_jsonrpc.ServeConn(conn)
	}
}

func (agent *ZincAgent) Get(key *string, value *string) error {
	Info("zinc agent get:%v", *key)
	t, err := agent.db.Get([]byte(*key))
	if err != nil {
		Error("query key:%s failed:%s", key, err)
		return err
	}
	if t == nil {
		*value = "[]"
		return nil
	}
	var data map[string]string
	if err = json.Unmarshal(t, &data); err != nil {
		Error("unmarshal key:%s failed:%v", key, err)
		return err
	}
	arr := make([]string, 2*len(data))
	i := 0
	for key, val := range data {
		arr[i] = key
		arr[i+1] = val
		i = i + 2
	}
	chunk, err := json.Marshal(arr)
	if err != nil {
		Error("marshal key:%s, failed:%v", key, err)
		return err
	}
	*value = string(chunk)
	return nil
}

func NewZincAgent(setting Setting, db *Leveldb) *ZincAgent {
	return &ZincAgent{nil, setting.Zinc.Addr, db}
}
