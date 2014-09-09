## env
depend on leveldb, snappy, levigo

please refer to [this blog] (http://xjdrew.github.io/blog/2014/09/09/build-static-leveldb-with-snappy/) for how to build a snappy enabled leveldb.

## Build
```
source env.sh
go install app
```

## Test
* start a redis-server listen on 127.0.0.1:6300
* bin/app conf/settings.json
* use a redis client, run command as follow

```
hset key1 v1 1
rename key1 key2
```

