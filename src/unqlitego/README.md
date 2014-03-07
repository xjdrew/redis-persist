unqlitego
=========

UnQLite Binding for golang.

Install
---------

```sh
$ go get github.com/nobonobo/unqlitego
```

Test
---------
```sh
$ go get github.com/r7kamura/gospel
$ cd ${GOPATH/:*/}/src/github.com/nobonobo/unqlitego
$ go test .
```

Benchmark
----------

```sh
$ go get github.com/r7kamura/gospel
$ cd ${GOPATH/:*/}/src/github.com/nobonobo/unqlitego
$ go test -bench Bench*
```

Output:(Macbook Air 2011 mid)

```
BenchmarkFileStore	  200000	      9667 ns/op
BenchmarkFileFetch	  500000	      7928 ns/op
BenchmarkMemStore	  500000	      3824 ns/op
BenchmarkMemFetch	 1000000	      3448 ns/op
```
