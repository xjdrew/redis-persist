package main
import (
    "log"
    "fmt"
    "os"
    "os/signal"
    "syscall"
    "flag"

    "conf"
    "redis"
    "gounqlite"
)

var config *conf.ConfigFile

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

    host,_ := config.GetString("redis", "host")
    password,_ := config.GetString("redis", "password")
    db,_ := config.GetInt("redis", "db")
    events,_ := config.GetString("redis", "events")
    channel,_ := config.GetString("redis", "channel")

    queue := make(chan string, 1024)
    
    cli1 := redis.NewRedis(host, password, db)
    m := NewMonitor(cli1, events, channel)
    

    uql_file,_ := config.GetString("unqlite", "file")
    uql, err := gounqlite.Open(uql_file)
    if err != nil {
        log.Fatalf("open unqlite db failed, file:%s, err:%v", uql_file, err)
        os.Exit(1)
    }

    cli2 := redis.NewRedis(host, password, db)
    s := NewStorer(cli2, uql)

    go func() {
        m.Start(queue)
    }()

    go func() {
        s.Start(queue)
    }()
    
    
    log.Println("start succeed")

    c := make(chan os.Signal, 1)
    signal.Notify(c, syscall.SIGKILL, syscall.SIGINT, syscall.SIGTERM)
    sig := <- c
    log.Printf("catch signal %v, program will exit",sig)
}

