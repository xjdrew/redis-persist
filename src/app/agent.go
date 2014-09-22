package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"io"
	"net"
	"sync"
)

type AgentHandler func(ud interface{}, params interface{}) (interface{}, error)

type AgentSvr struct {
	ln      net.Listener
	db      *Leveldb
	handers map[string][]interface{}
	wg      sync.WaitGroup
}

type Request struct {
	Id     uint32
	Method string
	Params interface{}
}

type Response struct {
	Id     uint32      `json:"id"`
	Result interface{} `json:"result"`
	Error  interface{} `json:"error"`
}

func (self *AgentSvr) dispatchRequst(conn *net.TCPConn, req *Request) {
	defer func() {
		if err := recover(); err != nil {
			Error("handle agent connection:%v failed:%v", conn.RemoteAddr(), err)
		}
	}()
	cb, ok := self.handers[req.Method]
	if ok {
		ud := cb[0]
		handler := cb[1].(AgentHandler)
		var resp Response
		resp.Id = req.Id
		if result, err := handler(ud, req.Params); err != nil {
			resp.Error = err
		} else {
			resp.Result = result
		}
		body, err := json.Marshal(resp)
		if err != nil {
			Panic("marshal response conn:%v, failed:%v", conn.RemoteAddr(), err)
		}

		length := uint32(len(body))
		buf := bytes.NewBuffer(nil)
		binary.Write(buf, binary.BigEndian, length)
		buf.Write(body)
		chunk := buf.Bytes()
		if _, err = conn.Write(chunk); err != nil {
			Panic("write response conn:%v, failed:%v", conn.RemoteAddr(), err)
		}
	} else {
		Error("unknown request:%v", req)
	}
}

func (self *AgentSvr) handleConnection(conn *net.TCPConn) {
	defer conn.Close()
	defer self.wg.Done()
	defer func() {
		if err := recover(); err != nil {
			Error("handle agent connection:%v failed:%v", conn.RemoteAddr(), err)
		}
	}()

	for {
		var sz uint32
		err := binary.Read(conn, binary.BigEndian, &sz)
		if err != nil {
			Error("read conn failed:%v, err:%v", conn.RemoteAddr(), err)
			break
		}
		buf := make([]byte, sz)
		_, err = io.ReadFull(conn, buf)
		if err != nil {
			Error("read conn failed:%v, err:%v", conn.RemoteAddr(), err)
			break
		}
		var req Request
		if err = json.Unmarshal(buf, &req); err != nil {
			Error("parse request failed:%v, err:%v", conn.RemoteAddr(), err)
		}

		go self.dispatchRequst(conn, &req)
	}
}

func (self *AgentSvr) Start() {
	ln, err := net.Listen("tcp", setting.Agent.Addr)
	if err != nil {
		Panic("resolve local addr failed:%s", err.Error())
	}
	Info("start agent succeed:%s", setting.Agent.Addr)

	// register handler
	self.Register("Get", self, handlerGet)

	self.ln = ln
	self.wg.Add(1)
	for {
		conn, err := self.ln.Accept()
		if err != nil {
			Error("accept failed:%v", err)
			continue
		}
		self.wg.Add(1)
		go self.handleConnection(conn.(*net.TCPConn))
	}
}

func (self *AgentSvr) Stop() {
	if self.ln != nil {
		self.ln.Close()
	}
	self.wg.Wait()
}

func (self *AgentSvr) Register(cmd string, ud interface{}, handler AgentHandler) {
	self.handers[cmd] = []interface{}{ud, handler}
}

func handlerGet(ud interface{}, params interface{}) (result interface{}, err error) {
	agent := ud.(*AgentSvr)
	key := params.(string)
	Info("agent get:%v", key)
	chunk, err := agent.db.Get([]byte(key))
	if chunk == nil || err != nil {
		Error("query key:%s failed:%v", key, err)
		return
	}
	var data map[string]string
	if err = json.Unmarshal(chunk, &data); err != nil {
		Error("unmarshal key:%s failed:%v", key, err)
		return
	}

	arr := make([]string, 2*len(data))
	i := 0
	for key, val := range data {
		arr[i] = key
		arr[i+1] = val
		i = i + 2
	}
	result = arr
	return
}

func NewAgent(db *Leveldb) *AgentSvr {
	agent := new(AgentSvr)
	agent.db = db
	agent.handers = make(map[string][]interface{})
	return agent
}
