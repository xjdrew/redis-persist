## Build
```
go install app
```

## Test
* start a redis-server listen on 127.0.0.1:6300
* bin/app conf/settings.conf
* use a redis client, run command as follow

```
hset key1 v1 1
rename key1 key2
```


