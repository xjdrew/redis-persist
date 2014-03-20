#!/bin/bash

export GOPATH=`pwd`
export CGO_CFLAGS=-I$GOPATH/include
export CGO_LDFLAGS=-L$GOPATH/bin
export LD_LIBRARY_PATH=$GOPATH/bin


