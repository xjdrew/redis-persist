package main

import (
    "log"
    "fmt"
    "bytes"
    // "strconv"
    "errors"
    "syscall"

    "encoding/json"
)

func shutdown(ud interface{}, args[] string) (result string, err error) {
    context := ud.(*Context)

    context.m.Stop()
    context.s.Stop()
    context.c.Stop()
    context.quit_chan <- syscall.SIGUSR1
    result = "done"
    return
}
func count(ud interface{}, args[] string) (result string, err error) {
    /*
    context := ud.(*Context)
    x := 0
    
    cursor,err := context.uql.NewCursor()
    if err != nil {
        return
    }
    for err := cursor.First();err == nil; err = cursor.Next() {
        x = x + 1
    }
    result = strconv.Itoa(x)
    */
    return 
}

func info(ud interface{}, args[] string) (result string, err error) {
    context := ud.(*Context)
    db := context.db

    key := ""
    if len(args) > 0 {
        key = args[0]
    }
    result = db.Info(key)
    return
}

func dump(ud interface{}, args[] string)(result string, err error) {
    if len(args) == 0 {
        err = errors.New("no key")
        return
    }

    key := args[0]
    context := ud.(*Context)
    db := context.db

    chunk, err := db.Get([]byte(key))
    if chunk == nil || err != nil {
        log.Printf("fetch data failed:%v", err)
        return
    }

    data := make(map[string] string)
    err = json.Unmarshal(chunk, &data)
    if err != nil {
        log.Printf("unmarshal chunk failed:%v", err)
        return
    }

    buf := bytes.NewBufferString("content:\n")
    for k,v := range data {
        fmt.Fprintf(buf, " %s -> %s\n", k, v)
    }
    result = buf.String()
    return
}

func diff(ud interface{}, args[] string) (result string, err error) {
    if len(args) == 0 {
        err = errors.New("no key")
        return
    }

    key := args[0]
    context := ud.(*Context)

    cli := context.redis
    db := context.db
    // query redis
    left := make(map[string] string)
    err = cli.Hgetall(key, left)
    if err != nil {
        return
    }

    chunk, err := db.Get([]byte(key))
    if chunk == nil || err != nil {
        log.Printf("fetch data failed:%v", err)
        return
    }

    right := make(map[string] string)
    err = json.Unmarshal(chunk, &right)
    if err != nil {
        log.Printf("unmarshal chunk failed:%v", err)
        return
    }

    
    buf := bytes.NewBufferString("left:redis, right:unqlite\n")
    for k,v1 := range left {
        if v2, ok := right[k];ok {
            if v1 != v2 {
                fmt.Fprintf(buf, "%s < %s, %s\n", k, v1, v2)
            }
        } else {
                fmt.Fprintf(buf, "%s, only in left\n", k)
        }
    }

    for k,_ := range right {
        if _, ok := left[k];!ok {
            fmt.Fprintf(buf, "%s, only in right\n", k)
        } 
    }
    result = buf.String()
    return
}

func (context *Context) Register(c *CmdService) {
    err := context.redis.Connect() 
    if err != nil {
        log.Panicf("register cmd service failed:%v", err)
    }

    log.Printf("register command service")
    // c.Register("count", context, count)
    c.Register("info", context, info)
    c.Register("dump", context, dump)
    c.Register("diff", context, diff)
    c.Register("shutdown", context, shutdown)
}

