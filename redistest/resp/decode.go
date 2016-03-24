// Package resp implements an efficient decoder for the Redis Serialization Protocol (RESP).
//
// See http://redis.io/topics/protocol for the reference.
package resp

import (
	"bytes"
	"errors"
	"fmt"
	"io"
)

var (
	// ErrInvalidPrefix is returned if the data contains an unrecognized prefix.
	ErrInvalidPrefix = errors.New("resp: invalid prefix")

	// ErrMissingCRLF is returned if a \r\n is missing in the data slice.
	ErrMissingCRLF = errors.New("resp: missing CRLF")

	// ErrInvalidInteger is returned if an invalid character is found while parsing an integer.
	ErrInvalidInteger = errors.New("resp: invalid integer character")

	// ErrInvalidBulkString is returned if the bulk string data cannot be decoded.
	ErrInvalidBulkString = errors.New("resp: invalid bulk string")

	// ErrInvalidArray is returned if the array data cannot be decoded.
	ErrInvalidArray = errors.New("resp: invalid array")

	// ErrNotAnArray is returned if the DecodeRequest function is called and
	// the decoded value is not an array.
	ErrNotAnArray = errors.New("resp: expected an array type")

	// ErrInvalidRequest is returned if the DecodeRequest function is called and
	// the decoded value is not an array containing only bulk strings, and at least 1 element.
	ErrInvalidRequest = errors.New("resp: invalid request, must be an array of bulk strings with at least one element")
)

// BytesReader defines the methods required for the Decode* family of methods.
// Notably, a *bufio.Reader and a *bytes.Buffer both satisfy this interface.
type BytesReader interface {
	io.Reader
	io.ByteReader
	ReadBytes(byte) ([]byte, error)
}

// Array represents an array of values, as defined by the RESP.
type Array []interface{}

// String is the Stringer implementation for the Array.
func (a Array) String() string {
	var buf bytes.Buffer
	for i, v := range a {
		buf.WriteString(fmt.Sprintf("[%2d] %[2]v (%[2]T)\n", i, v))
	}
	return buf.String()
}

// DecodeRequest decodes the provided byte slice and returns the array
// representing the request. If the encoded value is not an array, it
// returns ErrNotAnArray, and if it is not a valid request, it returns ErrInvalidRequest.
func DecodeRequest(r BytesReader) ([]string, error) {
	// Decode the value
	val, err := Decode(r)
	if err != nil {
		return nil, err
	}

	// Must be an array
	ar, ok := val.(Array)
	if !ok {
		return nil, ErrNotAnArray
	}

	// Must have at least one element
	if len(ar) < 1 {
		return nil, ErrInvalidRequest
	}

	// Must have only strings
	strs := make([]string, len(ar))
	for i, v := range ar {
		v, ok := v.(string)
		if !ok {
			return nil, ErrInvalidRequest
		}
		strs[i] = v
	}
	return strs, nil
}

// Decode decodes the provided byte slice and returns the parsed value.
func Decode(r BytesReader) (interface{}, error) {
	return decodeValue(r)
}

// decodeValue parses the byte slice and decodes the value based on its
// prefix, as defined by the RESP protocol.
func decodeValue(r BytesReader) (interface{}, error) {
	ch, err := r.ReadByte()
	if err != nil {
		return nil, err
	}

	var val interface{}
	switch ch {
	case '+':
		// Simple string
		val, err = decodeSimpleString(r)
	case '-':
		// Error
		val, err = decodeError(r)
	case ':':
		// Integer
		val, err = decodeInteger(r)
	case '$':
		// Bulk string
		val, err = decodeBulkString(r)
	case '*':
		// Array
		val, err = decodeArray(r)
	default:
		err = ErrInvalidPrefix
	}

	return val, err
}

// decodeArray decodes the byte slice as an array. It assumes the
// '*' prefix is already consumed.
func decodeArray(r BytesReader) (Array, error) {
	// First comes the number of elements in the array
	cnt, err := decodeInteger(r)
	if err != nil {
		return nil, err
	}
	switch {
	case cnt == -1:
		// Nil array
		return nil, nil

	case cnt == 0:
		// Empty, but allocated, array
		return Array{}, nil

	case cnt < 0:
		// Invalid length
		return nil, ErrInvalidArray

	default:
		// Allocate the array
		ar := make(Array, cnt)

		// Decode each value
		for i := 0; i < int(cnt); i++ {
			val, err := decodeValue(r)
			if err != nil {
				return nil, err
			}
			ar[i] = val
		}
		return ar, nil
	}
}

// decodeBulkString decodes the byte slice as a binary-safe string. The
// '$' prefix is assumed to be already consumed.
func decodeBulkString(r BytesReader) (interface{}, error) {
	// First comes the length of the bulk string, an integer
	cnt, err := decodeInteger(r)
	if err != nil {
		return nil, err
	}
	switch {
	case cnt == -1:
		// Special case to represent a nil bulk string
		return nil, nil

	case cnt < -1:
		return nil, ErrInvalidBulkString

	default:
		// Then the string is cnt long, and bytes read is cnt+n+2 (for ending CRLF)
		need := cnt + 2
		got := 0
		buf := make([]byte, need)
		for {
			nb, err := r.Read(buf[got:])
			if err != nil {
				return nil, err
			}
			got += nb
			if int64(got) == need {
				break
			}
		}
		return string(buf[:got-2]), err
	}
}

// decodeInteger decodes the byte slice as a singed 64bit integer. The
// ':' prefix is assumed to be already consumed.
func decodeInteger(r BytesReader) (val int64, err error) {
	var cr bool
	var sign int64 = 1
	var n int

loop:
	for {
		ch, err := r.ReadByte()
		if err != nil {
			return 0, err
		}
		n++

		switch ch {
		case '\r':
			cr = true
			break loop

		case '\n':
			break loop

		case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
			val = val*10 + int64(ch-'0')

		case '-':
			if n == 1 {
				sign = -1
				continue
			}
			fallthrough
		default:
			return 0, ErrInvalidInteger
		}
	}

	if !cr {
		return 0, ErrMissingCRLF
	}
	// Presume next byte was \n
	_, err = r.ReadByte()
	if err != nil {
		return 0, err
	}
	return sign * val, nil
}

// decodeSimpleString decodes the byte slice as a SimpleString. The
// '+' prefix is assumed to be already consumed.
func decodeSimpleString(r BytesReader) (interface{}, error) {
	v, err := r.ReadBytes('\r')
	if err != nil {
		return nil, err
	}
	// Presume next byte was \n
	_, err = r.ReadByte()
	if err != nil {
		return nil, err
	}
	return string(v[:len(v)-1]), nil
}

// decodeError decodes the byte slice as an Error. The '-' prefix
// is assumed to be already consumed.
func decodeError(r BytesReader) (interface{}, error) {
	return decodeSimpleString(r)
}
