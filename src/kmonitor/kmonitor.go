package main

import (
    "log"
    "fmt"
    "os"
    "os/signal"
    "flag"

    "conf"
    "redis"
)

var config *conf.ConfigFile
var quit bool = false

func run() int {
    host,_ := config.GetString("redis", "host")
    password,_ := config.GetString("redis", "password")
    db,_ := config.GetInt("redis", "db")

    events,_ := config.GetString("redis", "events")
    channel,_ := config.GetString("redis", "channel")

    cli := redis.NewRedis(host, password, db)
    err := cli.Connect()
    if err != nil {
        log.Print("connect to redis failed")
        return 1
    }

    _, err = cli.Exec("config", "set", "notify-keyspace-events", events)
    if err != nil {
        log.Printf("config redis failed:%v", err)
        return 1
    }
    
    _, err = cli.Exec("subscribe", channel)
    if err != nil {
        log.Printf("subscribe failed:%s", err)
        return 1
    }

    for {
        if quit {
            break
        }
        resp, err := cli.ReadResponse()
        if err != nil {
            log.Printf("read publish message failed:%v", err)
            return 2
        }
        if data, ok := resp.([]string); ok {
            if len(data) != 3 || data[0] != "message" {
                log.Printf("receive unexpected message, %v", data)
            } else {
                event := data[1]
                key := data[2]
                log.Printf("receive [%s], value[%s]", event, key)
            }
        } else {
            log.Printf("receive unexpected message, %v", resp)
        }
    }
    return 0
}

func usage() {
    fmt.Fprintf(os.Stderr, "usage: %s [config]\n", os.Args[0])
    flag.PrintDefaults()
    os.Exit(2)
}

func main() {
    flag.Usage = usage
    flag.Parse()
    
    args := flag.Args()
    if len(args) < 1 {
        fmt.Println("config file is missing.")
        os.Exit(1)
    }

    var err error
    config, err = conf.ReadConfigFile(args[0])
    if err != nil {
        fmt.Fprintf(os.Stderr, "read config file failed:%s", err)
        os.Exit(1)
    }
    
    c := make(chan os.Signal, 1)
    signal.Notify(c, os.Interrupt, os.Kill)
    go func() {
        s := <- c
        log.Printf("catch signal %v, program will exit",s)
        quit = true
    }()

    code := run()
    os.Exit(code)
}

