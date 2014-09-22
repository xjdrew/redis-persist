package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"
	"syscall"
)

type Context struct {
	db         *Leveldb
	m          *Monitor
	s          *StorerMgr
	c          *CmdService
	quit_chan  chan bool
	sync_queue chan string
}

type Redis struct {
	Host               string
	Password           string
	Db                 int
	NotificationConfig string
	Event              string
	Expire             bool
}

type LeveldbConfig struct {
	Dbname string
}

type Manager struct {
	Addr string
}

type Log struct {
	File  string
	Level int
}

type Agent struct {
	Addr string
}

type Setting struct {
	Redis   Redis
	Leveldb LeveldbConfig
	Manager Manager
	Log     Log
	Agent   Agent
}

func usage() {
	fmt.Fprintf(os.Stderr, "usage: %s [config]\n", os.Args[0])
	flag.PrintDefaults()
	os.Exit(2)
}

func handleSignal(quit chan bool) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	for sig := range c {
		switch sig {
		case syscall.SIGHUP:
			Error("catch sighup, ignore")
		default:
			quit <- true
		}
	}
}

var setting Setting

func main() {
	flag.Usage = usage
	flag.Parse()

	args := flag.Args()
	if len(args) < 1 {
		fmt.Println("config file is missing.")
		os.Exit(1)
	}

	content, err := ioutil.ReadFile(args[0])
	if err != nil {
		panic(err)
	}

	if err = json.Unmarshal([]byte(content), &setting); err != nil {
		panic(err)
	}

	// init log
	initLog()

	database := NewLeveldb(setting.Leveldb.Dbname)
	defer database.Close()

	m := NewMonitor()
	s := NewStorerMgr(database, 5)
	c := NewCmdService()
	agent := NewAgent(database)

	context := NewContext()
	context.db = database
	context.m = m
	context.s = s
	context.c = c
	context.Register(c)
	context.sync_queue = make(chan string, 1)

	go handleSignal(context.quit_chan)
	go m.Start(context.sync_queue)
	go s.Start(context.sync_queue)
	go c.Start()
	go agent.Start()

	Info("start succeed")
	Error("catch signal %v, program will exit", <-context.quit_chan)
}
