// Package redistest provides test helpers to manage a redis server.
package redistest

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/stretchr/testify/require"
)

// ClusterConfig is the configuration to use for servers started in
// redis-cluster mode. The value must contain a single reference to
// a string placeholder (%s), the port number.
var ClusterConfig = `
port %s
cluster-enabled yes
cluster-config-file nodes.%[1]s.conf
cluster-node-timeout 5000
appendonly no
`

// NumClusterNodes is the number of nodes started in a test cluster.
// When a cluster is started with replicas, there is 1 replica per
// cluster node, so the total number of nodes is NumClusterNodes * 2.
const NumClusterNodes = 3

// StartServer starts a redis-server instance on a free port.
// It returns the started *exec.Cmd and the port used. The caller
// should make sure to stop the command. If the redis-server
// command is not found in the PATH, the test is skipped.
//
// If w is not nil, both stdout and stderr of the server are
// written to it. If a configuration is specified, it is supplied
// to the server via stdin.
func StartServer(t *testing.T, w io.Writer, conf string) (*exec.Cmd, string) {
	if _, err := exec.LookPath("redis-server"); err != nil {
		t.Skip("redis-server not found in $PATH")
	}

	port := getFreePort(t)
	return startServerWithConfig(t, port, w, conf), port
}

// StartClusterWithReplicas starts a redis cluster of NumClusterNodes with
// 1 replica each. It returns the cleanup function to call after use
// (typically in a defer) and the list of ports for each node,
// masters first, then replicas.
func StartClusterWithReplicas(t *testing.T, w io.Writer) (func(), []string) {
	fn, ports := StartCluster(t, w)
	mapping := getClusterNodeIDs(t, ports...)

	var replicaPorts []string
	var replicaCmds []*exec.Cmd
	replicaMaster := make(map[string]string)
	for _, master := range ports {
		port := getClusterFreePort(t)
		cmd := startServerWithConfig(t, port, w, fmt.Sprintf(ClusterConfig, port))
		joinCluster(t, port, master)

		replicaPorts = append(replicaPorts, port)
		replicaCmds = append(replicaCmds, cmd)
		replicaMaster[port] = master
	}

	// wait for the cluster to stabilize
	require.True(t, waitForCluster(t, 10*time.Second, replicaPorts...), "wait for cluster replicas")
	for _, port := range replicaPorts {
		setupReplica(t, port, mapping[replicaMaster[port]])
	}
	// wait for replicas to join
	require.True(t, waitForReplicas(t, 10*time.Second, append(ports, replicaPorts...)...), "wait for cluster replicas")

	return func() {
		for _, c := range replicaCmds {
			c.Process.Kill()
		}
		for _, port := range replicaPorts {
			if strings.HasPrefix(port, ":") {
				port = port[1:]
			}
			os.Remove(filepath.Join(os.TempDir(), fmt.Sprintf("nodes.%s.conf", port)))
		}
		fn()
	}, append(ports, replicaPorts...)
}

// StartCluster starts a redis cluster of NumClusterNodes using the
// ClusterConfig variable as configuration. If w is not nil,
// stdout and stderr of each node will be written to it.
//
// It returns a function that should be called after the test
// (typically in a defer), and the list of ports for all nodes
// in the cluster.
func StartCluster(t *testing.T, w io.Writer) (func(), []string) {
	if _, err := exec.LookPath("redis-server"); err != nil {
		t.Skip("redis-server not found in $PATH")
	}

	const hashSlots = 16384

	cmds := make([]*exec.Cmd, NumClusterNodes)
	ports := make([]string, NumClusterNodes)
	slotsPerNode := hashSlots / NumClusterNodes

	for i := 0; i < NumClusterNodes; i++ {
		port := getClusterFreePort(t)
		cmd := startServerWithConfig(t, port, w, fmt.Sprintf(ClusterConfig, port))
		cmds[i], ports[i] = cmd, port

		// configure the cluster - add the slots and join
		var meetPort string
		if i > 0 {
			meetPort = ports[i-1]
		}
		countSlots := slotsPerNode
		if i == NumClusterNodes-1 {
			// add all remaining slots in the last node
			countSlots = hashSlots - (i * slotsPerNode)
		}
		setupClusterNode(t, port, i*slotsPerNode, countSlots)
		if meetPort != "" {
			joinCluster(t, port, meetPort)
		}
	}

	// wait for the cluster to catch up
	require.True(t, waitForCluster(t, 10*time.Second, ports...), "wait for cluster")

	return func() {
		for _, c := range cmds {
			c.Process.Kill()
		}
		for _, port := range ports {
			if strings.HasPrefix(port, ":") {
				port = port[1:]
			}
			os.Remove(filepath.Join(os.TempDir(), fmt.Sprintf("nodes.%s.conf", port)))
		}
	}, ports
}

func printClusterNodes(t *testing.T, port string) {
	conn, err := redis.Dial("tcp", ":"+port)
	require.NoError(t, err, "Dial to cluster node")
	defer conn.Close()

	res, err := conn.Do("CLUSTER", "NODES")
	require.NoError(t, err, "CLUSTER NODES")
	fmt.Println(string(res.([]byte)))
}

func printClusterSlots(t *testing.T, port string) {
	conn, err := redis.Dial("tcp", ":"+port)
	require.NoError(t, err, "Dial to cluster node")
	defer conn.Close()

	cmd := exec.Command("redis-cli", "-p", port, "CLUSTER", "SLOTS")
	b, err := cmd.CombinedOutput()
	require.NoError(t, err, "CLUSTER SLOTS via redis-cli")
	fmt.Println(string(b))
}

func joinCluster(t *testing.T, nodePort, clusterPort string) {
	conn, err := redis.Dial("tcp", ":"+nodePort)
	require.NoError(t, err, "Dial to node")
	defer conn.Close()

	// join the cluster
	_, err = conn.Do("CLUSTER", "MEET", "127.0.0.1", clusterPort)
	require.NoError(t, err, "CLUSTER MEET")
}

func getClusterNodeIDs(t *testing.T, ports ...string) map[string]string {
	if len(ports) == 0 {
		return nil
	}

	conn, err := redis.Dial("tcp", ":"+ports[0])
	require.NoError(t, err, "Dial to node")
	defer conn.Close()

	nodes, err := redis.String(conn.Do("CLUSTER", "NODES"))
	require.NoError(t, err, "CLUSTER NODES")

	mapping := make(map[string]string)
	s := bufio.NewScanner(strings.NewReader(nodes))
	for s.Scan() {
		fields := strings.Fields(s.Text())
		for _, port := range ports {
			if fields[1] == "127.0.0.1:"+port {
				mapping[port] = fields[0]
				break
			}
		}
	}
	require.Equal(t, len(ports), len(mapping), "Find IDs for all ports")
	return mapping
}

func setupReplica(t *testing.T, replicaPort, masterID string) {
	conn, err := redis.Dial("tcp", ":"+replicaPort)
	require.NoError(t, err, "Dial to replica node")
	defer conn.Close()

	_, err = conn.Do("CLUSTER", "REPLICATE", masterID)
	require.NoError(t, err, "CLUSTER REPLICATE")
}

func setupClusterNode(t *testing.T, port string, start, count int) {
	conn, err := redis.Dial("tcp", ":"+port)
	require.NoError(t, err, "Dial to cluster node")
	defer conn.Close()

	args := redis.Args{"ADDSLOTS"}
	for i := start; i < start+count; i++ {
		args = args.Add(i)
	}

	_, err = conn.Do("CLUSTER", args...)
	require.NoError(t, err, "CLUSTER ADDSLOTS")
}

func waitForReplicas(t *testing.T, timeout time.Duration, ports ...string) bool {
	deadline := time.Now().Add(timeout)

	for _, port := range ports {
		conn, err := redis.Dial("tcp", ":"+port)
		require.NoError(t, err, "Dial")

		for time.Now().Before(deadline) {
			v, err := redis.String(conn.Do("CLUSTER", "NODES"))
			require.NoError(t, err, "CLUSTER NODES")

			ms, rs := 0, 0
			s := bufio.NewScanner(strings.NewReader(v))
			for s.Scan() {
				fields := strings.Fields(s.Text())
				if fields[7] == "connected" {
					if strings.Contains(fields[2], "master") {
						ms++
					} else {
						rs++
					}
				}
			}
			if ms == NumClusterNodes && rs == NumClusterNodes {
				break
			}

			time.Sleep(100 * time.Millisecond)
		}
		conn.Close()

		if time.Now().After(deadline) {
			return false
		}
	}
	return true
}

func waitForCluster(t *testing.T, timeout time.Duration, ports ...string) bool {
	deadline := time.Now().Add(timeout)

	for _, port := range ports {
		conn, err := redis.Dial("tcp", ":"+port)
		require.NoError(t, err, "Dial")

		for time.Now().Before(deadline) {
			vals, err := redis.Bytes(conn.Do("CLUSTER", "INFO"))
			require.NoError(t, err, "CLUSTER INFO")
			if bytes.Contains(vals, []byte("cluster_state:ok")) {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		conn.Close()

		if time.Now().After(deadline) {
			return false
		}
	}
	return true
}

func startServerWithConfig(t *testing.T, port string, w io.Writer, conf string) *exec.Cmd {
	var args []string
	if conf == "" {
		args = []string{"--port", port}
	} else {
		args = []string{"-"}
	}
	c := exec.Command("redis-server", args...)
	c.Dir = os.TempDir()

	if w != nil {
		c.Stderr = w
		c.Stdout = w
	}
	if conf != "" {
		c.Stdin = strings.NewReader(conf)
	}

	// start the server
	require.NoError(t, c.Start(), "start redis-server")

	// wait for the server to start accepting connections
	require.True(t, waitForPort(port, 10*time.Second), "wait for redis-server")

	t.Logf("redis-server started on port %s", port)
	return c
}

func waitForPort(port string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", ":"+port, time.Second)
		if err == nil {
			conn.Close()
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

func getClusterFreePort(t *testing.T) string {
	const maxPort = 55535

	// the port number in a redis-cluster must be below 55535 because
	// the nodes communicate with others on port p+10000. Try to get
	// lucky and subtract 10000 from the random port received if it
	// is too high.
	port := getFreePort(t)
	if n, _ := strconv.Atoi(port); n >= maxPort {
		port = strconv.Itoa(n - 10000)
	}
	return port
}

func getFreePort(t *testing.T) string {
	l, err := net.Listen("tcp", ":0")
	require.NoError(t, err, "listen on port 0")
	defer l.Close()
	_, p, err := net.SplitHostPort(l.Addr().String())
	require.NoError(t, err, "parse host and port")
	return p
}

// NewPool creates a redis pool to return connections on the specified
// addr.
func NewPool(t *testing.T, addr string) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     2,
		MaxActive:   10,
		IdleTimeout: time.Minute,
		Dial: func() (redis.Conn, error) {
			return redis.Dial("tcp", addr)
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
}
