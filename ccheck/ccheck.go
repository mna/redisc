// Command ccheck implements the consistency checker redis cluster client
// as described in http://redis.io/topics/cluster-tutorial. It is used
// to test the redisc package with real cluster failover and resharding
// situations.
package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/mna/redisc"
)

var (
	addrFlag = flag.String("addr", "localhost:7000", "Redis server `address`.")

	connTimeoutFlag  = flag.Duration("c", time.Second, "Connection `timeout`.")
	delayFlag        = flag.Duration("d", 0, "Delay `duration` between INCR calls.")
	idleTimeoutFlag  = flag.Duration("i", 30*time.Second, "Pooled connection idle `timeout`.")
	readTimeoutFlag  = flag.Duration("r", 100*time.Millisecond, "Read `timeout`.")
	writeTimeoutFlag = flag.Duration("w", 100*time.Millisecond, "Write `timeout`.")

	maxIdleFlag   = flag.Int("max-idle", 10, "Maximum idle `connections` per pool.")
	maxActiveFlag = flag.Int("max-active", 100, "Maximum active `connections` per pool.")
)

const (
	workingSet = 1000
	keySpace   = 10000
)

var (
	mu sync.Mutex

	writes, reads             int
	failedWrites, failedReads int
	lostWrites, noAckWrites   int
)

func main() {
	flag.Parse()
	rand.Seed(time.Now().UnixNano())

	cluster := &redisc.Cluster{
		StartupNodes: []string{*addrFlag},
		DialOptions: []redis.DialOption{
			redis.DialConnectTimeout(*connTimeoutFlag),
			redis.DialReadTimeout(*readTimeoutFlag),
			redis.DialWriteTimeout(*writeTimeoutFlag),
		},
		CreatePool: createPool,
	}
	defer cluster.Close()

	errCh := make(chan error, 1)
	go printStats()
	go printErr(errCh)

	runChecks(cluster, errCh, *delayFlag)
}

func getRetryConn(cluster *redisc.Cluster) redis.Conn {
	c := cluster.Get()
	c, _ = redisc.RetryConn(c, 4, 100*time.Millisecond)
	if err := c.Err(); err != nil {
		log.Fatalf("failed to get a connection: %v", err)
	}
	return c
}

func runChecks(cluster *redisc.Cluster, errCh chan<- error, delay time.Duration) {
	var c redis.Conn

	cache := make(map[string]int, workingSet)
	for {
		var r, w, fr, fw, lw, naw int

		if c == nil {
			c = getRetryConn(cluster)
		}

		key := genKey()

		// read only if we know what that key should be
		exp, ok := cache[key]
		if ok {
			v, err := redis.Int(c.Do("GET", key))
			if err != nil {
				if isNetOpError(err) {
					c.Close()
					c = nil
					continue
				}

				select {
				case errCh <- fmt.Errorf("read from slot %d failed: %v", redisc.Slot(key), err):
				default:
				}
				fr = 1
			} else {
				r = 1
				if exp > v {
					lw = exp - v
				} else if exp < v {
					naw = v - exp
				}
			}
		}

		// write
		v, err := redis.Int(c.Do("INCR", key))
		if err != nil {
			if isNetOpError(err) {
				c.Close()
				c = nil
				continue
			}

			select {
			case errCh <- fmt.Errorf("write to slot %d failed: %v", redisc.Slot(key), err):
			default:
			}
			fw = 1
		} else {
			w = 1
			cache[key] = v
		}

		updateStats(w, r, fw, fr, lw, naw)
		time.Sleep(delay)
	}
}

func isNetOpError(err error) bool {
	_, ok := err.(*net.OpError)
	return ok
}

func updateStats(deltas ...int) {
	mu.Lock()
	writes += deltas[0]
	reads += deltas[1]
	failedWrites += deltas[2]
	failedReads += deltas[3]
	lostWrites += deltas[4]
	noAckWrites += deltas[5]
	mu.Unlock()
}

func printErr(errCh <-chan error) {
	for err := range errCh {
		fmt.Println(err)
		time.Sleep(time.Second)
	}
}

// each second, print stats
func printStats() {
	for range time.Tick(time.Second) {
		mu.Lock()
		w, r := writes, reads
		fw, fr := failedWrites, failedReads
		lw, naw := lostWrites, noAckWrites
		mu.Unlock()
		fmt.Printf("%d R (%d err) | %d W (%d err) | %d lost | %d noack\n", r, fr, w, fw, lw, naw)
	}
}

func genKey() string {
	ks := workingSet
	if rand.Float64() > 0.5 {
		ks = keySpace
	}
	return "key_" + strconv.Itoa(rand.Intn(ks))
}

func createPool(addr string, opts ...redis.DialOption) (*redis.Pool, error) {
	return &redis.Pool{
		Dial: func() (redis.Conn, error) {
			return redis.Dial("tcp", addr, opts...)
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
		MaxActive:   *maxActiveFlag,
		MaxIdle:     *maxIdleFlag,
		IdleTimeout: *idleTimeoutFlag,
	}, nil
}
