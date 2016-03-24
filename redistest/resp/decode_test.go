package resp

import (
	"bytes"
	"io"
	"reflect"
	"testing"
)

var decodeErrCases = []struct {
	enc []byte
	val interface{}
	err error
}{
	0:  {[]byte("+ceci n'est pas un string"), nil, io.EOF},
	1:  {[]byte("+"), nil, io.EOF},
	2:  {[]byte("-ceci n'est pas un string"), nil, io.EOF},
	3:  {[]byte("-"), nil, io.EOF},
	4:  {[]byte(":123\n"), int64(0), ErrMissingCRLF},
	5:  {[]byte(":123a\r\n"), int64(0), ErrInvalidInteger},
	6:  {[]byte(":123"), int64(0), io.EOF},
	7:  {[]byte(":-1-3\r\n"), int64(0), ErrInvalidInteger},
	8:  {[]byte(":"), int64(0), io.EOF},
	9:  {[]byte("$"), nil, io.EOF},
	10: {[]byte("$6\r\nc\r\n"), nil, io.EOF},
	11: {[]byte("$6\r\nabc\r\n"), nil, io.EOF},
	12: {[]byte("$6\nabc\r\n"), nil, ErrMissingCRLF},
	13: {[]byte("$4\r\nabc\r\n"), nil, io.EOF},
	14: {[]byte("$-3\r\n"), nil, ErrInvalidBulkString},
	15: {[]byte("*1\n:10\r\n"), Array(nil), ErrMissingCRLF},
	16: {[]byte("*-3\r\n"), Array(nil), ErrInvalidArray},
	17: {[]byte(":\r\n"), int64(0), nil},
	18: {[]byte("$\r\n\r\n"), "", nil},
}

var decodeValidCases = []struct {
	enc []byte
	val interface{}
	err error
}{
	0:  {[]byte{'+', '\r', '\n'}, "", nil},
	1:  {[]byte{'+', 'a', '\r', '\n'}, "a", nil},
	2:  {[]byte{'+', 'O', 'K', '\r', '\n'}, "OK", nil},
	3:  {[]byte("+ceci n'est pas un string\r\n"), "ceci n'est pas un string", nil},
	4:  {[]byte{'-', '\r', '\n'}, "", nil},
	5:  {[]byte{'-', 'a', '\r', '\n'}, "a", nil},
	6:  {[]byte{'-', 'K', 'O', '\r', '\n'}, "KO", nil},
	7:  {[]byte("-ceci n'est pas un string\r\n"), "ceci n'est pas un string", nil},
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
		Array{"string", "error", int64(-2345)}, nil},
	19: {[]byte("*5\r\n+string\r\n-error\r\n:-2345\r\n$4\r\nallo\r\n*2\r\n$0\r\n\r\n$-1\r\n"),
		Array{"string", "error", int64(-2345), "allo",
			Array{"", nil}}, nil},
}

var decodeRequestCases = []struct {
	raw []byte
	exp []string
	err error
}{
	0: {[]byte("*-1\r\n"), nil, ErrInvalidRequest},
	1: {[]byte(":4\r\n"), nil, ErrNotAnArray},
	2: {[]byte("*0\r\n"), nil, ErrInvalidRequest},
	3: {[]byte("*1\r\n:6\r\n"), nil, ErrInvalidRequest},
	4: {[]byte("*1\r\n$2\r\nab\r\n"), []string{"ab"}, nil},
	5: {[]byte("*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$24\r\nceci n'est pas un string\r\n"),
		[]string{"SET", "mykey", "ceci n'est pas un string"}, nil},
}

func TestDecode(t *testing.T) {
	for i, c := range append(decodeValidCases, decodeErrCases...) {
		got, err := Decode(bytes.NewBuffer(c.enc))
		if err != c.err {
			t.Errorf("%d: expected error %v, got %v", i, c.err, err)
		}
		if got == nil && c.val == nil {
			continue
		}
		assertValue(t, i, got, c.val)
	}
}

func TestDecodeRequest(t *testing.T) {
	for i, c := range decodeRequestCases {
		buf := bytes.NewBuffer(c.raw)
		got, err := DecodeRequest(buf)
		if err != c.err {
			t.Errorf("%d: expected error %v, got %v", i, c.err, err)
		}
		if got == nil && c.exp == nil {
			continue
		}
		assertValue(t, i, got, c.exp)
	}
}

func assertValue(t *testing.T, i int, got, exp interface{}) {
	tgot, texp := reflect.TypeOf(got), reflect.TypeOf(exp)
	if tgot != texp {
		t.Errorf("%d: expected type %s, got %s", i, texp, tgot)
	}
	if !reflect.DeepEqual(got, exp) {
		t.Errorf("%d: expected output %v, got %v", i, exp, got)
	}
}

var forbenchmark interface{}

func BenchmarkDecodeSimpleString(b *testing.B) {
	var val interface{}
	var err error

	for i := 0; i < b.N; i++ {
		r := bytes.NewBuffer(decodeValidCases[3].enc)
		val, err = Decode(r)
	}
	if err != nil {
		b.Fatal(err)
	}
	forbenchmark = val
}

func BenchmarkDecodeError(b *testing.B) {
	var val interface{}
	var err error

	for i := 0; i < b.N; i++ {
		r := bytes.NewBuffer(decodeValidCases[7].enc)
		val, err = Decode(r)
	}
	if err != nil {
		b.Fatal(err)
	}
	forbenchmark = val
}

func BenchmarkDecodeInteger(b *testing.B) {
	var val interface{}
	var err error

	for i := 0; i < b.N; i++ {
		r := bytes.NewBuffer(decodeValidCases[10].enc)
		val, err = Decode(r)
	}
	if err != nil {
		b.Fatal(err)
	}
	forbenchmark = val
}

func BenchmarkDecodeBulkString(b *testing.B) {
	var val interface{}
	var err error

	for i := 0; i < b.N; i++ {
		r := bytes.NewBuffer(decodeValidCases[13].enc)
		val, err = Decode(r)
	}
	if err != nil {
		b.Fatal(err)
	}
	forbenchmark = val
}

func BenchmarkDecodeArray(b *testing.B) {
	var val interface{}
	var err error

	for i := 0; i < b.N; i++ {
		r := bytes.NewBuffer(decodeValidCases[19].enc)
		val, err = Decode(r)
	}
	if err != nil {
		b.Fatal(err)
	}
	forbenchmark = val
}

func BenchmarkDecodeRequest(b *testing.B) {
	var val interface{}
	var err error

	for i := 0; i < b.N; i++ {
		r := bytes.NewBuffer(decodeRequestCases[5].raw)
		val, err = Decode(r)
	}
	if err != nil {
		b.Fatal(err)
	}
	forbenchmark = val
}
