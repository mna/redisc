package redisc

import "github.com/garyburd/redigo/redis"

const hashSlots = 16384

// Cluster manages a redis cluster.
type Cluster struct {
	// StartupNodes is the list of initial nodes that make up
	// the cluster. The values can be only the address and port
	// part (e.g.: "111.222.333.444:6379") or a URL with a scheme
	// specifying the network type to use (e.g.: "tcp://localhost:6379"
	// or "unix:///tmp/redis.sock").
	//
	// By default, tcp is used. See the redis documentation on how
	// to enable other network interfaces such as unix sockets.
	StartupNodes []string

	// DialOptions is the list of options to set on each new connection
	// if no redis.Pool is used (if CreatePool is nil). If CreatePool is
	// not nil, then this is the list of options that will be passed
	// as the options parameter, which may or may not be used by the
	// CreatePool function.
	DialOptions []redis.DialOption

	// CreatePool is the function to call to create a redis.Pool for
	// the specified network and address, using the provided options
	// as set in DialOptions. If this function is not nil, then a
	// redis.Pool is created for each node in the cluster and the
	// pool is used to manage the connections. If it is nil, then
	// redis.Dial is called to get new connections.
	CreatePool func(network, address string, options ...redis.DialOption) (*redis.Pool, error)
}
