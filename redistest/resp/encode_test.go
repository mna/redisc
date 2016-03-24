package resp

import (
	"bytes"
	"testing"
)

var encodeValidCases = []struct {
	enc []byte
	val interface{}
	err error
}{
	0:  {[]byte{'+', '\r', '\n'}, SimpleString(""), nil},
	1:  {[]byte{'+', 'a', '\r', '\n'}, SimpleString("a"), nil},
	2:  {[]byte{'+', 'O', 'K', '\r', '\n'}, SimpleString("OK"), nil},
	3:  {[]byte("+ceci n'est pas un string\r\n"), SimpleString("ceci n'est pas un string"), nil},
	4:  {[]byte{'-', '\r', '\n'}, Error(""), nil},
	5:  {[]byte{'-', 'a', '\r', '\n'}, Error("a"), nil},
	6:  {[]byte{'-', 'K', 'O', '\r', '\n'}, Error("KO"), nil},
	7:  {[]byte("-ceci n'est pas un string\r\n"), Error("ceci n'est pas un string"), nil},
	8:  {[]byte(":1\r\n"), int64(1), nil},
	9:  {[]byte(":123\r\n"), int64(123), nil},
	10: {[]byte(":-123\r\n"), int64(-123), nil},
	11: {[]byte("$0\r\n\r\n"), "", nil},
	12: {[]byte("$24\r\nceci n'est pas un string\r\n"), "ceci n'est pas un string", nil},
	13: {[]byte("$51\r\nceci n'est pas un string\r\navec\rdes\nsauts\r\nde\x00ligne.\r\n"), "ceci n'est pas un string\r\navec\rdes\nsauts\r\nde\x00ligne.", nil},
	14: {[]byte("$-1\r\n"), nil, nil},
	15: {[]byte("*0\r\n"), Array{}, nil},
	16: {[]byte("*1\r\n:10\r\n"), Array{int64(10)}, nil},
	17: {[]byte("*-1\r\n"), Array(nil), nil},
	18: {[]byte("*3\r\n+string\r\n-error\r\n:-2345\r\n"),
		Array{SimpleString("string"), Error("error"), int64(-2345)}, nil},
	19: {[]byte("*5\r\n+string\r\n-error\r\n:-2345\r\n$4\r\nallo\r\n*2\r\n$0\r\n\r\n$-1\r\n"),
		Array{SimpleString("string"), Error("error"), int64(-2345), "allo",
			Array{"", nil}}, nil},
}

func TestEncode(t *testing.T) {
	var buf bytes.Buffer
	var err error

	for i, c := range encodeValidCases {
		buf.Reset()
		err = Encode(&buf, c.val)
		if err != nil {
			t.Errorf("%d: got error %s", i, err)
			continue
		}
		if bytes.Compare(buf.Bytes(), c.enc) != 0 {
			t.Errorf("%d: expected %x (%q), got %x (%q)", i, c.enc, string(c.enc), buf.Bytes(), buf.String())
		}
	}
}

func BenchmarkEncodeSimpleString(b *testing.B) {
	var err error
	var buf bytes.Buffer
	for i := 0; i < b.N; i++ {
		err = Encode(&buf, encodeValidCases[3].val)
	}
	if err != nil {
		b.Fatal(err)
	}
}

func BenchmarkEncodeError(b *testing.B) {
	var err error
	var buf bytes.Buffer
	for i := 0; i < b.N; i++ {
		err = Encode(&buf, encodeValidCases[7].val)
	}
	if err != nil {
		b.Fatal(err)
	}
}

func BenchmarkEncodeInteger(b *testing.B) {
	var err error
	var buf bytes.Buffer
	for i := 0; i < b.N; i++ {
		err = Encode(&buf, encodeValidCases[10].val)
	}
	if err != nil {
		b.Fatal(err)
	}
}

func BenchmarkEncodeBulkString(b *testing.B) {
	var err error
	var buf bytes.Buffer
	for i := 0; i < b.N; i++ {
		err = Encode(&buf, encodeValidCases[13].val)
	}
	if err != nil {
		b.Fatal(err)
	}
}

func BenchmarkEncodeArray(b *testing.B) {
	var err error
	var buf bytes.Buffer
	for i := 0; i < b.N; i++ {
		err = Encode(&buf, encodeValidCases[19].val)
	}
	if err != nil {
		b.Fatal(err)
	}
}
