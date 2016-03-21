package redisc

import (
	"errors"
	"strconv"
	"sync"

	"github.com/garyburd/redigo/redis"
)

const hashSlots = 16384

// Cluster manages a redis cluster.
type Cluster struct {
	// StartupNodes is the list of initial nodes that make up
	// the cluster. The values are expected as "address:port"
	// (e.g.: "111.222.333.444:6379").
	StartupNodes []string

	// DialOptions is the list of options to set on each new connection
	// if no redis.Pool is used (if CreatePool is nil). If CreatePool is
	// not nil, then this is the list of options that will be passed
	// as the options parameter, which may or may not be used by the
	// CreatePool function.
	DialOptions []redis.DialOption

	// CreatePool is the function to call to create a redis.Pool for
	// the specified TCP address, using the provided options
	// as set in DialOptions. If this function is not nil, then a
	// redis.Pool will be created for each node in the cluster and the
	// pool is used to manage the connections. If it is nil, then
	// redis.Dial is called to get new connections.
	CreatePool func(address string, options ...redis.DialOption) (*redis.Pool, error)

	mu      sync.Mutex // protects following fields
	err     error
	pools   map[string]*redis.Pool
	nodes   map[string]bool
	mapping [hashSlots]string // hash slot number to master server address
}

// RefreshMapping calls the CLUSTER SLOTS redis command and updates
// the cluster's internal mapping of hash slots to redis node. It calls
// the command on each startup nodes, in order, until one of them succeeds.
//
// It should typically be called after creating the Cluster and before
// using it. The cluster automatically keeps its mapping up-to-date
// when in use, based on the redis commands MOVED responses.
func (c *Cluster) RefreshMapping() error {
	c.mu.Lock()
	if err := c.err; err != nil {
		c.mu.Unlock()
		return err
	}

	if len(c.nodes) == 0 {
		c.populateNodes()
	}

	// grab a slice of addresses so we don't hold on to the lock during
	// the CLUSTER SLOTS calls.
	addrs := make([]string, 0, len(c.nodes))
	for addr := range c.nodes {
		addrs = append(addrs, addr)
	}
	c.mu.Unlock()

	return c.refreshMapping(addrs)
}

func (c *Cluster) refreshMapping(addrs []string) error {
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
			c.mu.Unlock()

			return nil
		}
	}
	return errors.New("redisc: all nodes failed")
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

func (c *Cluster) populateNodes() {
	c.nodes = make(map[string]bool)
	for _, n := range c.StartupNodes {
		c.nodes[n] = true
	}
}

// Get returns a redis.Conn interface that can be used to call
// redis commands on the cluster. The application must close the
// returned connection. The actual network connection is only
// attempted on the first call to Do or Send, using the command's
// key to determine the hash slot and the node to use to execute
// that command. If Receive is called before any Do or Send, then
// a random node is selected in the cluster, assuming the Receive
// call is for pub-sub listening (in which case any node is ok).
//
// All commands sent using the same connection must live in the same
// hash slot, otherwise the request will fail. See the redis cluster
// documentation for how the hash of related keys can be controlled
// so that they live on the same node.
func (c *Cluster) Get() redis.Conn {
	return nil
}

// GetForKeySlot returns a redis connection for the node holding
// the specified key's slot. This can be useful when the key(s)
// to handle are known in advance, but keyless commands need
// to be run first (e.g. MULTI).
func (c *Cluster) GetForKeySlot(key string) redis.Conn {
	// TODO : something like that... return c.getConnForAddr(c.mapping[slot])
	return nil
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
