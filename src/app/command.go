package main

import (
    "log"
    "net"
    "bufio"
    "strings"
    "runtime"
)

type CmdHandler func(ud interface{}, args []string) (string, error)

type CmdService struct {
    ln net.Listener
    addr string
    handlers map[string] []interface{}
    quit_chan chan int
}

func (c *CmdService) handleConnection(conn net.Conn) {
    log.Printf("handle conn:%v", conn)
    reader := bufio.NewReader(conn)
    for {
        s, err := reader.ReadString('\n')
        if err != nil {
            log.Printf("read conn:%v failed, err:%v", conn, err)
            break
        }
        s = strings.Trim(s, "\r\n ")
        args := strings.Split(s, " ")
        if len(args) == 0 {
            continue
        }

        var response string

        cmd := args[0]
        cb,ok := c.handlers[cmd]
        if ok {
            log.Printf("recv command: %s", cmd)
            ud := cb[0]
            handle := cb[1].(CmdHandler)
            result, err := handle(ud, args[1:])
            if err != nil {
                response = "- " + err.Error()
            } else {
                response = "+ " + result
            }
        } else {
            response = "- unknown command: " + cmd
        }
        response = response + "\n"
        conn.Write([]byte(response))
    }
    log.Printf("end handle conn:%v", conn)
}

func (c *CmdService) Register(cmd string, ud interface{}, handler CmdHandler) {
    _, ok := c.handlers[cmd]
    if handler == nil && ok {
        delete(c.handlers, cmd)
    } else {
        log.Printf("register cmd:%s", cmd)
        c.handlers[cmd] = []interface{}{ud, handler}
    }
}

func (c *CmdService) Start() {
    ln, err := net.Listen("tcp", c.addr)
    if err != nil {
        log.Panicf("start manager failed:%v", err)
    }

    log.Printf("start manager succeed:%s", c.addr)
    
    c.ln = ln
    for {
        conn, err := c.ln.Accept()
        if err != nil {
            log.Printf("accept failed:%v", err)
            break
        }
        runtime.SetFinalizer(conn, (*net.TCPConn).Close)
        go c.handleConnection(conn)
    }
    c.quit_chan <- 1
}

func (c *CmdService) Stop() {
    if c.ln != nil {
        c.ln.Close()
    }
    <- c.quit_chan
}

func NewCmdService(addr string) *CmdService {
    return &CmdService{nil, addr, make(map[string] []interface{}), make(chan int)}
}

