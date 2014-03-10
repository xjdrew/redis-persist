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
    "unqlitego"
)

type Context struct {
    uql *unqlitego.Database
    redis *redis.Redis
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
    

    uql_file,_ := config.GetString("unqlite", "file")
    uql, err := unqlitego.NewDatabase(uql_file)
    if err != nil {
        log.Fatalf("open unqlite db failed, file:%s, err:%v", uql_file, err)
    } else {
        log.Printf("open unqlite db succeed, file:%s", uql_file)
    } 

    defer uql.Close()

    cli2 := redis.NewRedis(host, password, db)
    s := NewStorer(cli2, uql)
    
    addr,_ := config.GetString("manager", "addr")
    c := NewCmdService(addr)

    cli3 := redis.NewRedis(host, password, db)
    context := &Context{uql, cli3}
    context.Register(c)

    go m.Start(queue)
    go s.Start(queue)
    go c.Start()
    
    log.Println("start succeed")

    quit_flag := make(chan os.Signal, 1)
    signal.Notify(quit_flag, syscall.SIGINT, syscall.SIGTERM)
    sig := <- quit_flag
    log.Printf("catch signal %v, program will exit",sig)
}

