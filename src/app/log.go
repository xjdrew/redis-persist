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
