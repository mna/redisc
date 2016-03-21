package redisc

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHashSlotForKey(t *testing.T) {
	cases := []struct {
		in  string
		out int
	}{
		{"", 0},
		{"a", 15495},
		{"b", 3300},
		{"ab", 13567},
		{"abc", 7638},
		{"a{b}", 3300},
		{"{a}b", 15495},
		{"{a}{b}", 15495},
		{"{}{a}{b}", 11267},
		{"a{b}c", 3300},
		{"{a}bc", 15495},
		{"{a}{b}{c}", 15495},
		{"{}{a}{b}{c}", 1044},
		{"a{bc}d", 12685},
		{"a{bcd}", 1872},
		{"{abcd}", 10294},
		{"abcd", 10294},
		{"{a", 10276},
		{"a}", 5921},
		{"123456789", 12739},
		{"a≠b", 11870},
		{"•", 97},
		{"a{}{b}c", 14872},
	}

	for _, c := range cases {
		got := HashSlotForKey(c.in)
		assert.Equal(t, c.out, got, c.in)
	}
}
