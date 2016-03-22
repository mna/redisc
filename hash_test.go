package redisc

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSlot(t *testing.T) {
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
		got := Slot(c.in)
		assert.Equal(t, c.out, got, c.in)
	}
}

func TestSplitBySlot(t *testing.T) {
	cases := []struct {
		// join/split by comma, for convenience
		in  string
		out []string
	}{
		{"", nil},
		{"a", []string{"a"}},
		{"a,b", []string{"b", "a"}},
		{"a,b,cb{a}", []string{"b", "a,cb{a}"}},
		{"a,b,cb{a},a{b}", []string{"b,a{b}", "a,cb{a}"}},
		{"a,b,cb{a},a{b},abc", []string{"b,a{b}", "abc", "a,cb{a}"}},
	}

	for _, c := range cases {
		args := strings.Split(c.in, ",")
		if c.in == "" {
			args = nil
		}
		got := SplitBySlot(args...)

		exp := make([][]string, len(c.out))
		for i, o := range c.out {
			exp[i] = strings.Split(o, ",")
		}

		assert.Equal(t, exp, got, c.in)
		t.Logf("%#v", got)
	}
}
