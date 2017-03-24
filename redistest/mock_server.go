package redistest

import (
	"bufio"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/mna/redisc/redistest/resp"
	"github.com/stretchr/testify/require"
)

// MockServer is a mock redis server.
type MockServer struct {
	Addr string

	done chan struct{}
	wg   sync.WaitGroup
	h    func(string, ...string) interface{}
	t    *testing.T
	l    net.Listener
}

// StartMockServer creates and starts a mock redis server. The handler is
// called for each command received by the server. The returned value is
// encoded in the redis protocol and sent to the client. The caller should close
// the server after use.
func StartMockServer(t *testing.T, handler func(cmd string, args ...string) interface{}) *MockServer {
	l, err := net.Listen("tcp", ":0")
	require.NoError(t, err, "net.Listen")

	_, port, _ := net.SplitHostPort(l.Addr().String())
	s := &MockServer{
		Addr: ":" + port,
		done: make(chan struct{}),
		h:    handler,
		t:    t,
		l:    l,
	}
	go s.serve()
	return s
}

// Close closes the mock redis server.
func (s *MockServer) Close() {
	select {
	case <-s.done:
		return
	default:
	}

	require.NoError(s.t, s.l.Close(), "Close listener")
	<-s.done
	exit := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(exit)
	}()

	// wait for a few seconds for connections to finish, otherwise fail
	select {
	case <-exit:
		return
	case <-time.After(5 * time.Second):
		s.t.Fatal("failed to cleanly stop the mock server")
	}
}

func (s *MockServer) serve() {
	defer close(s.done)
	for {
		conn, err := s.l.Accept()
		if err != nil {
			return
		}
		s.wg.Add(1)
		go s.serveConn(conn)
	}
}

func (s *MockServer) serveConn(c net.Conn) {
	defer s.wg.Done()

	go func() {
		<-s.done
		c.Close()
	}()

	br := bufio.NewReader(c)
	for {
		// Get the request
		ar, err := resp.DecodeRequest(br)
		if err != nil {
			return
		}

		// Handle the response
		v := s.h(ar[0], ar[1:]...)
		if err := resp.Encode(c, v); err != nil {
			panic(err)
		}
	}
}
