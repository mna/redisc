package redisc

import (
	"strings"
	"testing"

	"github.com/PuerkitoBio/juggler/internal/redistest"
	"github.com/stretchr/testify/assert"
)

func TestRefreshNonCluster(t *testing.T) {
	cmd, port := redistest.StartServer(t, nil, "")
	defer cmd.Process.Kill()

	c := &Cluster{
		StartupNodes: []string{":" + port},
	}
	err := c.Refresh()
	if assert.Error(t, err, "Refresh") {
		assert.Contains(t, err.Error(), "redisc: all nodes failed", "expected error message")
	}
}

func TestRefreshCluster(t *testing.T) {
	fn, ports := redistest.StartCluster(t, nil)
	defer fn()

	for i, p := range ports {
		ports[i] = ":" + p
	}
	c := &Cluster{
		StartupNodes: ports,
	}

	err := c.Refresh()
	if assert.NoError(t, err, "Refresh") {
		var prev string
		pix := -1
		for ix, master := range c.mapping {
			if master != prev {
				prev = master
				t.Logf("%5d: %s\n", ix, master)
				pix++
			}
			if assert.NotEmpty(t, master) {
				split := strings.Index(master, ":")
				assert.Contains(t, ports, master[split:], "expected master")
			}
		}
	}
}
