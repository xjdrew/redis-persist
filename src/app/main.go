package main
import (
    "log"
    "fmt"
    "io"
    "os"
    "os/signal"
    "syscall"
    "flag"

    "conf"
    "redis"
)

type Context struct {
    db    *Leveldb
    redis *redis.Redis
    m     *Monitor
    s     *Storer
    c     *CmdService
    quit_chan chan os.Signal
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

    config, err := conf.ReadConfigFile(args[0])
    if err != nil {
        fmt.Fprintf(os.Stderr, "read config file failed:%s", err)
        os.Exit(1)
    }

    logfile,_ := config.GetString("log", "file")
    fp,err := os.OpenFile(logfile, os.O_RDWR | os.O_APPEND | os.O_CREATE, 0666)
    if err != nil {
        fmt.Fprintf(os.Stderr, "open log file failed:%s", err)
        os.Exit(1)
    }
    defer fp.Close()
    log.SetOutput(io.MultiWriter(fp, os.Stderr))

    host,_ := config.GetString("redis", "host")
    password,_ := config.GetString("redis", "password")
    db,_ := config.GetInt("redis", "db")
    events,_ := config.GetString("redis", "events")
    channel,_ := config.GetString("redis", "channel")

    queue := make(chan string, 1024)

    cli1 := redis.NewRedis(host, password, db)
    m := NewMonitor(cli1, events, channel)


    dbname,err := config.GetString("leveldb", "dbname")
    if err != nil {
        log.Fatalf("get leveldb config failed:%v", err)
    }

    database := NewLeveldb()
    err = database.Open(dbname)
    if err != nil {
        log.Panicf("open db failed, err:%v", err)
    } else {
        log.Printf("open db succeed, dbname:%v", dbname)
    }

    defer database.Close()

    cli2 := redis.NewRedis(host, password, db)
    s := NewStorer(cli2, database)

    addr,_ := config.GetString("manager", "addr")
    c := NewCmdService(addr)

    cli3 := redis.NewRedis(host, password, db)
    context := &Context{database, cli3, m, s, c, make(chan os.Signal)}
    context.Register(c)

    addr, _ = config.GetString("zinc", "addr")
    zinc_agent := NewZincAgent(addr)

    signal.Notify(context.quit_chan, syscall.SIGINT, syscall.SIGTERM)

    go m.Start(queue)
    go s.Start(queue)
    go c.Start()
    go zinc_agent.Start()

    log.Println("start succeed")
    log.Printf("catch signal %v, program will exit",<-context.quit_chan)
}

