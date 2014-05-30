package main

import (
    "os"
    "log"
    "net"
    "net/rpc"
    "net/rpc/jsonrpc"
)

type ZincAgent struct {
    listener net.Listener
    addr string
    quit_chan chan int
}

func (agent *ZincAgent) Start() {
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
    for {
        conn, err := listener.Accept()
        if err != nil {
            continue
        }
        jsonrpc.ServeConn(conn)
    }
}

func (agent *ZincAgent) Get(key *string, value *string) error {
    log.Printf("zinc agent get:%v", *key)
    *value = *key
    return nil
}

func NewZincAgent(addr string) *ZincAgent {
    return &ZincAgent{nil, addr, make(chan int)}
}
