package redis

import "testing"

func TestRedis(t *testing.T) {
    cli := NewRedis("127.0.0.1:6300", "foobared", 2)
    err := cli.Connect()
    if err != nil {
        t.Errorf("connect to redis failed:%v", err)
    }
    
    _, err = cli.Exec("keys", "*")
    if err != nil {
        t.Errorf("hgetall failed:%v", err)
    }
}

