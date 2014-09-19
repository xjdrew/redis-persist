package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
)

func finiLog(fp *os.File) {
	fmt.Print("finilog\n")
	fp.Close()
}

func initLog() {
	fp, err := os.OpenFile(setting.Log.File, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0666)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open log file failed:%s", err)
		os.Exit(1)
	}
	log.SetOutput(io.MultiWriter(fp, os.Stderr))
	runtime.SetFinalizer(fp, finiLog)
}

func _print(format string, a ...interface{}) {
	log.Printf(format, a...)
}

func Debug(format string, a ...interface{}) {
	if setting.Log.Level > 2 {
		_print(format, a...)
	}
}

func Info(format string, a ...interface{}) {
	if setting.Log.Level > 1 {
		_print(format, a...)
	}
}

func Error(format string, a ...interface{}) {
	if setting.Log.Level > 0 {
		_print(format, a...)
	}
}

func Panic(format string, a ...interface{}) {
	_print(format, a...)
	panic("!!")
}
