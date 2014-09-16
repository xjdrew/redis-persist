package main

import (
	"custom_jsonrpc"
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
	*value = string(t)
	return nil
}

func NewZincAgent(setting Setting, db *Leveldb) *ZincAgent {
	return &ZincAgent{nil, setting.Zinc.Addr, db}
}
