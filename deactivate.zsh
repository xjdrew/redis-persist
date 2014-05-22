#!/usr/bin/env zsh

export GOPATH=$OLDGOPATH
unset OLDGOPATH
export CGO_CFLAGS=-I$GOPATH/include
export CGO_LDFLAGS=-L$GOPATH/bin
export LD_LIBRARY_PATH=$GOPATH/bin
