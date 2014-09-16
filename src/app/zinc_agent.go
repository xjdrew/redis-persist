package main

import (
	"custom_jsonrpc"
	"encoding/json"
	"log"
	"net"
	"net/rpc"
	"os"
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
		log.Printf("Error: %v", err)
		os.Exit(1)
	}
	listener, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		log.Printf("Error: %v", err)
		os.Exit(1)
	}
	log.Printf("Start json rpc on %v", tcpAddr)
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("accept conn failed:%v", err)
			continue
		}
		log.Printf("New conn:%v", conn)
		go custom_jsonrpc.ServeConn(conn)
	}
}

func (agent *ZincAgent) Get(key *string, value *string) error {
	log.Printf("zinc agent get:%v", *key)
	t, err := agent.db.Get([]byte(*key))
	if err != nil {
		log.Printf("query key:%s failed:%s", key, err)
		return err
	}
	var data map[string]string
	if err = json.Unmarshal(t, &data); err != nil {
		log.Println(err)
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
		log.Println(err)
		return err
	}
	*value = string(chunk)
	return nil
}

func NewZincAgent(setting Setting, db *Leveldb) *ZincAgent {
	return &ZincAgent{nil, setting.Zinc.Addr, db}
}
