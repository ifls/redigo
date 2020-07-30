package redis_test

import (
	`context`
	`log`
	`testing`
	`time`

	`github.com/stretchr/testify/assert`

	`github.com/gomodule/redigo/redis`
)

var unlockScript = redis.NewScript(2, `
	local key     = KEYS[1]
	local content = KEYS[2]
	local value = redis.call('get', key)
	if value == content then
	    return redis.call('del', key);
	end
	return 0
`)

func tryLock(conn redis.Conn, key string, value string) (ok bool, err error) {
	_, err = redis.String(conn.Do("SET", key, value, "EX", int(30), "NX"))
	if err == redis.ErrNil {
		// The lock was not successful, it already exists.
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// Unlock 解锁 改成script 保证原子性
func UnlockScript(conn redis.Conn, key string, value string) (ok bool, err error) {
	// 放回连接池
	// defer conn.Close()

	rsp, err := unlockScript.Do(conn, key, value)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("%T, %+v\n", rsp, rsp)
	result, ok := rsp.(int64)
	if !ok {
		log.Fatal("not int64")
	}
	return result == 1, nil
}

func TestLock(t *testing.T) {
	conn, err := redis.Dial("tcp", "127.0.0.1:6379")
	if err != nil {
		t.Fatal(err)
	}
	key := "lock_key"
	val := "lock_val"
	ok, err := tryLock(conn, key, val)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second)
	assert.Equal(t, true, ok)

	valrsp, err := conn.Do("get", key)
	if err != nil {
		t.Fatal(err)
	}

	log.Printf("1 %T, %s\n", valrsp, valrsp)

	valrsp2, err := conn.Do("ttl", key)
	if err != nil {
		t.Fatal(err)
	}

	log.Printf("2 %T, %+v\n", valrsp2, valrsp2)

	ok, err = tryLock(conn, key, val+"ss")
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second)
	assert.Equal(t, false, ok)

	valrsp, err = conn.Do("get", key)
	if err != nil {
		t.Fatal(err)
	}

	log.Printf("3 %T, %s\n", valrsp, valrsp)

	valrsp2, err = conn.Do("ttl", key)
	if err != nil {
		t.Fatal(err)
	}

	log.Printf("4 %T, %+v\n", valrsp2, valrsp2)

	ok, err = UnlockScript(conn, key, val+"xx")
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second)
	assert.Equal(t, false, ok)
	valrsp, err = conn.Do("get", key)
	if err != nil {
		t.Fatal(err)
	}

	log.Printf("5 %T, %s\n", valrsp, valrsp)

	valrsp2, err = conn.Do("ttl", key)
	if err != nil {
		t.Fatal(err)
	}

	log.Printf("6 %T, %+v\n", valrsp2, valrsp2)

	ok, err = UnlockScript(conn, key, val)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second)
	assert.Equal(t, true, ok)
	valrsp, err = conn.Do("get", key)
	if err != nil {
		t.Fatal(err)
	}

	log.Printf("7 %T, %+v\n", valrsp, valrsp)

	valrsp2, err = conn.Do("ttl", key)
	if err != nil {
		t.Fatal(err)
	}

	log.Printf("8 %T, %+v\n", valrsp2, valrsp2)
}

// NewPools 新连接池
func NewPools(addr string) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     10,
		IdleTimeout: 180 * time.Second,
		Dial: func() (c redis.Conn, err error) {
			c, err = redis.Dial("tcp", addr)
			if err != nil {
				return nil, err
			}

			return c, nil
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			if time.Since(t) < time.Minute {
				return nil
			}
			_, err := c.Do("PING")
			return err
		},
	}
}

func TestPool(t *testing.T) {
	pool := NewPools("127.0.0.1:6379")

	conn, err := pool.GetContext(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	rsp, err := conn.Do("get", "key1")
	if err != nil {
		t.Fatal(err)
	}

	log.Fatal(rsp)
}
