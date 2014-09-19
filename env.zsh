#!/usr/bin/env zsh

export OLDGOPATH=$GOPATH
export GOPATH=`pwd`
export CGO_CFLAGS=-I$GOPATH/include
export CGO_LDFLAGS=-L$GOPATH/bin
export LD_LIBRARY_PATH=$GOPATH/bin
export GOMAXPROCS=3
