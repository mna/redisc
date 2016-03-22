package redisc

import (
	"errors"
	"math/rand"
	"strconv"
	"sync"
	"time"

	"github.com/garyburd/redigo/redis"
)

var rnd = rand.New(rand.NewSource(time.Now().UnixNano()))

const hashSlots = 16384

// Cluster manages a redis cluster. If the CreatePool field is not nil,
// a redis.Pool is used for each node in the cluster to get connections
// via Cluster.Get. If it is nil or if Cluster.Dial is called, redis.Dial
// is used to get the connection.
type Cluster struct {
	// StartupNodes is the list of initial nodes that make up
	// the cluster. The values are expected as "address:port"
	// (e.g.: "111.222.333.444:6379").
	StartupNodes []string

	// DialOptions is the list of options to set on each new connection.
	DialOptions []redis.DialOption

	// CreatePool is the function to call to create a redis.Pool for
	// the specified TCP address, using the provided options
	// as set in DialOptions. If this field is not nil, a
	// redis.Pool is created for each node in the cluster and the
	// pool is used to manage the connections unless Cluster.Dial
	// is called.
	CreatePool func(address string, options ...redis.DialOption) (*redis.Pool, error)

	// MaxAttempts is the maximum number of attempts allowed when
	// running a command on a connection returned by RetryConn, in
	// order to automatically follow MOVED or ASK redirections. The
	// default value of 0 means no limit.
	MaxAttempts int

	mu            sync.Mutex             // protects following fields
	err           error                  // broken connection error
	pools         map[string]*redis.Pool // created pools per node
	nodes         map[string]bool        // set of known active nodes, kept up-to-date
	mapping       [hashSlots]string      // hash slot number to master server address
	refreshNeeded bool                   // refresh mapping on next command
}

// Refresh updates the cluster's internal mapping of hash slots
// to redis node. It calls CLUSTER SLOTS on each known node until one
// of them succeeds.
//
// It should typically be called after creating the Cluster and before
// using it. The cluster automatically keeps its mapping up-to-date
// afterwards, based on the redis commands' MOVED responses.
func (c *Cluster) Refresh() error {
	c.mu.Lock()
	err := c.err
	if err == nil {
		c.refreshNeeded = true // avoid creating concurrent refresh goroutines
	}
	c.mu.Unlock()
	if err != nil {
		return err
	}

	return c.refresh()
}

func (c *Cluster) refresh() error {
	addrs := c.getNodeAddrs()
	for _, addr := range addrs {
		m, err := c.getClusterSlots(addr)
		if err == nil {
			// succeeded, save as mapping
			c.mu.Lock()
			// mark all current nodes as false
			for k := range c.nodes {
				c.nodes[k] = false
			}
			for _, sm := range m {
				for ix := sm.start; ix <= sm.end; ix++ {
					c.mapping[ix] = sm.master
					c.nodes[sm.master] = true
				}
			}
			// remove all nodes that are gone from the cluster
			for k, ok := range c.nodes {
				if !ok {
					delete(c.nodes, k)
				}
			}
			// mark that no refresh is needed until another MOVED
			c.refreshNeeded = false
			c.mu.Unlock()

			return nil
		}
	}
	return errors.New("redisc: all nodes failed")
}

// needsRefresh handles automatic update of the mapping.
func (c *Cluster) needsRefresh(re *RedirError) {
	c.mu.Lock()
	c.mapping[re.NewSlot] = re.Addr
	if !c.refreshNeeded {
		// refreshNeeded is reset to false only once the goroutine has
		// finished updating the mapping, so a new refresh goroutine
		// will only be started if none is running.
		c.refreshNeeded = true
		go c.refresh()
	}
	c.mu.Unlock()
}

type slotMapping struct {
	start, end int
	master     string
}

func (c *Cluster) getClusterSlots(addr string) ([]slotMapping, error) {
	conn, err := c.getConnForAddr(addr)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	vals, err := redis.Values(conn.Do("CLUSTER", "SLOTS"))
	if err != nil {
		return nil, err
	}

	m := make([]slotMapping, 0, len(vals))
	for len(vals) > 0 {
		var slotRange []interface{}
		vals, err = redis.Scan(vals, &slotRange)
		if err != nil {
			return nil, err
		}

		var start, end int
		var node []interface{}
		if _, err = redis.Scan(slotRange, &start, &end, &node); err != nil {
			return nil, err
		}
		var addr string
		var port int
		if _, err = redis.Scan(node, &addr, &port); err != nil {
			return nil, err
		}

		m = append(m, slotMapping{start: start, end: end, master: addr + ":" + strconv.Itoa(port)})
	}

	return m, nil
}

func (c *Cluster) getConnForAddr(addr string) (redis.Conn, error) {
	// non-pooled doesn't require a lock
	if c.CreatePool == nil {
		return redis.Dial("tcp", addr, c.DialOptions...)
	}

	c.mu.Lock()

	p := c.pools[addr]
	if p == nil {
		c.mu.Unlock()
		pool, err := c.CreatePool(addr, c.DialOptions...)
		if err != nil {
			return nil, err
		}

		c.mu.Lock()
		c.pools[addr] = pool
		p = pool
	}

	c.mu.Unlock()

	conn := p.Get()
	return conn, conn.Err()
}

func (c *Cluster) getConnForSlot(slot int) (redis.Conn, error) {
	c.mu.Lock()
	addr := c.mapping[slot]
	c.mu.Unlock()
	if addr == "" {
		return nil, errors.New("redisc: no node for slot " + strconv.Itoa(slot))
	}
	return c.getConnForAddr(addr)
}

func (c *Cluster) getRandomConn() (redis.Conn, error) {
	addrs := c.getNodeAddrs()
	perms := rnd.Perm(len(addrs))
	for _, ix := range perms {
		addr := addrs[ix]
		conn, err := c.getConnForAddr(addr)
		if err == nil {
			return conn, nil
		}
	}
	return nil, errors.New("redisc: failed to get a connection")
}

func (c *Cluster) getConn(preferredSlot int) (conn redis.Conn, err error) {
	if preferredSlot >= 0 {
		conn, err = c.getConnForSlot(preferredSlot)
	}
	if preferredSlot < 0 || err != nil {
		conn, err = c.getRandomConn()
	}
	return conn, err
}

func (c *Cluster) getNodeAddrs() []string {
	c.mu.Lock()

	// populate nodes lazily, only once
	if c.nodes == nil {
		c.nodes = make(map[string]bool)
		for _, n := range c.StartupNodes {
			c.nodes[n] = true
		}
	}

	// grab a slice of addresses
	addrs := make([]string, 0, len(c.nodes))
	for addr := range c.nodes {
		addrs = append(addrs, addr)
	}
	c.mu.Unlock()

	return addrs
}

// Dial returns a connection the same way as Cluster.Get, but
// it guarantees that the connection will not be managed by the
// pool, even if Cluster.CreatePool is set. The actual returned
// type is *redisc.Conn, see its documentation for details.
func (c *Cluster) Dial() (redis.Conn, error) {
	c.mu.Lock()
	err := c.err
	c.mu.Unlock()

	if err != nil {
		return nil, err
	}

	return &Conn{
		cluster:   c,
		forceDial: true,
	}, nil
}

// Get returns a redis.Conn interface that can be used to call
// redis commands on the cluster. The application must close the
// returned connection. The actual returned type is *redisc.Conn,
// see its documentation for details.
func (c *Cluster) Get() redis.Conn {
	c.mu.Lock()
	err := c.err
	c.mu.Unlock()

	return &Conn{
		cluster: c,
		err:     err,
	}
}

// Close releases the resources used by the cluster. It closes all the
// pools that were created, if any.
func (c *Cluster) Close() error {
	c.mu.Lock()
	err := c.err
	if err == nil {
		c.err = errors.New("redisc: closed")
		for _, p := range c.pools {
			if e := p.Close(); e != nil && err == nil {
				err = e
			}
		}
	}
	c.mu.Unlock()

	return err
}
