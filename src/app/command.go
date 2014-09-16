package main

import (
	"bufio"
	"log"
	"net"
	"strings"
	"sync"
)

type CmdHandler func(ud interface{}, args []string) (string, error)

type CmdService struct {
	ln       net.Listener
	addr     string
	handlers map[string][]interface{}
	wg       sync.WaitGroup
}

func (c *CmdService) handleConnection(conn net.Conn) {
	defer conn.Close()
	defer c.wg.Done()
	defer func() {
		if err := recover(); err != nil {
			log.Printf("handle connection:%v failed:%v", conn.RemoteAddr(), err)
		}
	}()

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
		cb, ok := c.handlers[cmd]
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
	// no need add one, save one for the last connection
	defer c.wg.Done()

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
		c.wg.Add(1)
		go c.handleConnection(conn)
	}
}

func (c *CmdService) Stop() {
	if c.ln != nil {
		c.ln.Close()
	}
	c.wg.Wait()
}

func NewCmdService() *CmdService {
	cmdService := new(CmdService)
	cmdService.addr = setting.Manager.Addr
	cmdService.handlers = make(map[string][]interface{})
	return cmdService
}
