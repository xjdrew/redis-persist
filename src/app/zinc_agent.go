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
            continue
        }
        log.Printf("New conn:%v", conn)
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
