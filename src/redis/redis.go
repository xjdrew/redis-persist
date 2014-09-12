package redis

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"log"
	"net"
	"strconv"
)

type Redis struct {
	addr     string
	password string
	db       int
	conn     net.Conn
}

var UnsupportedArgType = errors.New("unsupported arg type")
var MalformedResponse = errors.New("malformed response")
var NoConnection = errors.New("no connection")

func composeMessage(cmd string, args []interface{}) ([]byte, error) {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "*%d\r\n", len(args)+1)
	fmt.Fprintf(&buf, "$%d\r\n%s\r\n", len(cmd), cmd)
	for _, arg := range args {
		var v string
		if str, ok := arg.(string); ok {
			v = str
		} else if str, ok := arg.(int); ok {
			v = strconv.Itoa(str)
		} else {
			return nil, UnsupportedArgType
		}

		fmt.Fprintf(&buf, "$%d\r\n%s\r\n", len(v), v)
	}
	return buf.Bytes(), nil
}

func readBulkString(reader *bufio.Reader, sz int) (str string, err error) {
	if sz < 0 {
		return
	}

	var buf = make([]byte, sz+2)
	var p = buf
	for {
		var n int
		n, err = reader.Read(p)
		if err != nil {
			return
		}
		if n < len(p) {
			p = p[n:]
		} else {
			break
		}
	}
	str = string(buf[:sz])
	// log.Printf("string:%s", str)
	return
}

func readResponse(reader *bufio.Reader) (interface{}, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}

	content := line[1 : len(line)-2]
	switch line[0] {
	case '-':
		return nil, errors.New(content)
	case '+':
		return content, nil
	case ':':
		return strconv.Atoi(content)
	case '$':
		sz, _ := strconv.Atoi(content)
		return readBulkString(reader, sz)
	case '*':
		sz, _ := strconv.Atoi(content)
		if sz < 0 {
			return nil, nil
		}
		var ret = make([]string, sz)
		for i := 0; i < sz; i++ {
			nextline, err := reader.ReadString('\n')
			// log.Printf("header:%s", nextline)
			if err != nil {
				return nil, err
			}
			nextcontent := nextline[1 : len(nextline)-2]
			if nextline[0] == ':' {
				ret[i] = nextcontent
			} else if nextline[0] == '$' {
				sz, _ := strconv.Atoi(nextcontent)
				s, err := readBulkString(reader, sz)
				if err != nil {
					return nil, err
				}
				ret[i] = s
			} else {
				log.Printf("unexpected response(*): %s", nextline)
				return nil, MalformedResponse
			}
		}
		return ret, nil
	}
	log.Printf("unexpected response():%s", line)
	return nil, MalformedResponse
}

// for pub/sub, don't call it directly
func (r *Redis) ReadResponse() (interface{}, error) {
	reader := bufio.NewReader(r.conn)
	return readResponse(reader)
}

func (r *Redis) Exec(cmd string, args ...interface{}) (interface{}, error) {
	if r.conn == nil {
		return nil, NoConnection
	}

	data, err := composeMessage(cmd, args)
	if err != nil {
		return nil, err
	}

	_, err = r.conn.Write(data)
	if err != nil {
		return nil, err
	}
	reader := bufio.NewReader(r.conn)
	return readResponse(reader)
}

func (r *Redis) Hget(key string, subkey string) (resp string, err error) {
    result, err := r.Exec("hget", key, subkey)
	if err != nil {
		return
	}
    resp = result.(string)
	return
}

func (r *Redis) Hgetall(key string, obj map[string]string) (err error) {
	resp, err := r.Exec("hgetall", key)
	if err != nil {
		return
	}

	if values, ok := resp.([]string); ok {
		sz := len(values)
		// if sz 不为偶数，丢弃最后一项
		for i := 0; i < sz-1; i = i + 2 {
			obj[values[i]] = values[i+1]
		}
	}
	return
}

func (r *Redis) Hgetall_arr(key string) (resp []string, err error) {
	t, err := r.Exec("hgetall", key)
	if err != nil {
		return
	}

	resp = t.([]string)
	return
}

func (r *Redis) Type(key string) (name string, err error) {
	resp, err := r.Exec("type", key)
	if err != nil {
		return
	}
	name = resp.(string)
	return
}

func (r *Redis) Connect() (err error) {
	log.Printf("connect to redis:%s", r.addr)

	if r.conn != nil {
		return
	}

	r.conn, err = net.Dial("tcp", r.addr)
	if err != nil {
		return
	}

	if r.password != "" {
		_, err = r.Exec("auth", r.password)
		if err != nil {
			return
		}
	}

	_, err = r.Exec("select", r.db)
	return
}

func (r *Redis) Close() {
	if r.conn != nil {
		r.conn.Close()
		r.conn = nil
	}
}

func (r *Redis) ReConnect() error {
	r.Close()
	return r.Connect()
}

// new function
func NewRedis(addr string, password string, db int) *Redis {
	return &Redis{addr: addr, password: password, db: db}
}
