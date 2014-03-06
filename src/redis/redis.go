package redis

import (
    "net"
    "bytes"
    "fmt"
    "strconv"
    "bufio"
    "errors"
    )

type Redis struct {
    addr string
    password string
    db int
    conn net.Conn
}

func composeMessage(cmd string, args []interface{}) ([] byte, error) {
    var buf bytes.Buffer
    fmt.Fprintf(&buf, "*%d\r\n", len(args) + 1)
    fmt.Fprintf(&buf, "$%d\r\n%s\r\n", len(cmd), cmd)
    for _, arg := range args {
        var v string
        if str, ok := arg.(string); ok {
            v = str
        } else if str, ok := arg.(int); ok {
            v = strconv.Itoa(str)
        } else {
            return nil, errors.New("unsupported arg type")
        }

        fmt.Fprintf(&buf, "$%d\r\n%s\r\n", len(v), v)
    }
    return buf.Bytes(), nil
}

func readBuldString(reader *bufio.Reader, sz int) (str string, err error) {
    if sz < 0 {
        return
    }
    
    var buf = make([]byte, sz+2)
    _,err = reader.Read(buf)
    if err != nil {
        return
    }
    str = string(buf[:sz])
    return
}

func readResponse(reader *bufio.Reader) (interface{}, error) {
    line, err := reader.ReadString('\n')
    if err != nil {
        return nil, err
    }

    content := line[1:len(line) - 2]
    switch line[0] {
        case '-':
            return nil, errors.New(content)
        case '+':
            return content, nil
        case ':':
            return strconv.Atoi(content)
        case '$':
            sz,_ := strconv.Atoi(content)
            return readBuldString(reader, sz)
        case '*':
            sz,_ := strconv.Atoi(content)
            if sz < 0 {
                return nil, nil
            }
            var ret = make([]string, sz)
            for i := 0; i < sz; i++ {
                nextline,err := reader.ReadString('\n')
                if err != nil {
                    return nil, err
                }
                nextcontent := nextline[1:len(nextline)-2]
                if nextline[0] == ':' {
                    ret[i] = nextcontent
                } else if nextline[0] == '$' {
                    sz,_ := strconv.Atoi(nextcontent)
                    s, err := readBuldString(reader, sz)
                    if err != nil {
                        return nil, err
                    }
                    ret[i] = s
                } else {
                    return nil, errors.New("unexpected response(*):" + nextline)
                }
            }
            return ret, nil
    }
    return nil, errors.New("unexpected response():" + line)
}

// for pub/sub, don't call it directly
func (r *Redis) ReadResponse()(interface{}, error) {
    reader := bufio.NewReader(r.conn)
    return readResponse(reader)
}

func (r *Redis) Exec(cmd string, args ... interface{}) (interface {}, error) {
    data,err := composeMessage(cmd, args) 
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

func (r *Redis) Connect() (err error) {
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

    _,err = r.Exec("select", r.db)
    return
}

func (r *Redis) ReConnect() (err error) {
    if r.conn != nil {
        r.conn.Close()
        r.conn = nil
    }
    return r.Connect()
}

// new function
func NewRedis(addr string, password string, db int) *Redis {
    return &Redis{addr : addr, password : password, db : db}
}

