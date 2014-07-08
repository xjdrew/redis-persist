package main
import (
    "log"
    "fmt"
    "io"
    "io/ioutil"
    "strconv"
    "os"
    "os/signal"
    "syscall"
    "flag"
    "encoding/json"
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

type Redis struct {
    Host string
    Password string
    Db string
    Events string
    Channel string
}

type LeveldbConfig struct {
    Dbname string
}

type Manager struct {
    Addr string
}

type Log struct {
    File string
}

type Zinc struct {
    Addr string
}

type Config struct {
    Redis Redis
    Leveldb LeveldbConfig
    Manager Manager
    Log Log
    Zinc Zinc
}

func main() {
    flag.Usage = usage
    flag.Parse()

    args := flag.Args()
    if len(args) < 1 {
        fmt.Println("config file is missing.")
        os.Exit(1)
    }

    var config Config
    content, err := ioutil.ReadFile(args[1])
    if err != nil {
        panic(err)
    }
    if err = json.Unmarshal([]byte(content), &config); err != nil {
        panic(err)
    }
    logfile := config.Log.File
    host := config.Redis.Host
    password := config.Redis.Password
    tdb,_ := strconv.ParseInt(config.Redis.Db, 0, 0)
    db := int(tdb)
    events := config.Redis.Events
    channel := config.Redis.Channel
    dbname := config.Leveldb.Dbname
    manager_addr := config.Manager.Addr
    zinc_addr := config.Zinc.Addr

    fp,err := os.OpenFile(logfile, os.O_RDWR | os.O_APPEND | os.O_CREATE, 0666)
    if err != nil {
        fmt.Fprintf(os.Stderr, "open log file failed:%s", err)
        os.Exit(1)
    }
    defer fp.Close()
    log.SetOutput(io.MultiWriter(fp, os.Stderr))


    queue := make(chan string, 1024)

    cli1 := redis.NewRedis(host, password, db)
    m := NewMonitor(cli1, events, channel)

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

    c := NewCmdService(manager_addr)

    cli3 := redis.NewRedis(host, password, db)
    context := &Context{database, cli3, m, s, c, make(chan os.Signal)}
    context.Register(c)

    zinc_agent := NewZincAgent(zinc_addr, database)

    signal.Notify(context.quit_chan, syscall.SIGINT, syscall.SIGTERM)

    go m.Start(queue)
    go s.Start(queue)
    go c.Start()
    go StartZincAgent(zinc_agent)

    log.Println("start succeed")
    log.Printf("catch signal %v, program will exit",<-context.quit_chan)
}

