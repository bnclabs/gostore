package lib

import "unsafe"
import "reflect"
import "fmt"
import "errors"
import "bytes"
import "strings"
import "encoding/json"

// TODO: FailsafeRequest and FailsafePost can be localized as gen-server
// methods. That way we can be specific about the channel type instead of
// typing it as `chan []interface{}`.
// TODO: ResponseError can also be localized to gen-server routine.

// Parsecsv convert a string of command seperated value into list of string of
// values.
func Parsecsv(input string) []string {
	if input == "" {
		return nil
	}
	ss := strings.Split(input, ",")
	outs := make([]string, 0)
	for _, s := range ss {
		s = strings.Trim(s, " \t\r\n")
		if s == "" {
			continue
		}
		outs = append(outs, s)
	}
	return outs
}

// Memcpy copy memory block of length `ln` from `src` to `dst`. This
// function is useful if memory block is obtained outside golang runtime.
func Memcpy(dst, src unsafe.Pointer, ln int) int {
	var srcnd, dstnd []byte
	srcsl := (*reflect.SliceHeader)(unsafe.Pointer(&srcnd))
	srcsl.Len, srcsl.Cap = ln, ln
	srcsl.Data = (uintptr)(unsafe.Pointer(src))
	dstsl := (*reflect.SliceHeader)(unsafe.Pointer(&dstnd))
	dstsl.Len, dstsl.Cap = ln, ln
	dstsl.Data = (uintptr)(unsafe.Pointer(dst))
	return copy(dstnd, srcnd)
}

// FailsafeRequest for gen-server design pattern. While posting a request to
// reqch channel, if channel is full but gen-server has exited or crashed,
// prevent caller from blocking. Similarly, while waiting for a response from
// respch channel, if gen-server has exited or crashed, prevent caller from
// blocking.
func FailsafeRequest(reqch, respch chan []interface{}, cmd []interface{}, finch chan bool) ([]interface{}, error) {
	select {
	case reqch <- cmd:
		if respch != nil {
			select {
			case resp := <-respch:
				return resp, nil
			case <-finch:
				return nil, errors.New("server closed")
			}
		}
	case <-finch:
		return nil, errors.New("server closed")
	}
	return nil, nil
}

// FailsafePost for gen-server design pattern. While posting a message to
// reqch channel, if i/p channel is full but gen-server has exited or crashed,
// prevent caller from blocking.
func FailsafePost(reqch chan []interface{}, cmd []interface{}, finch chan bool) error {
	select {
	case reqch <- cmd:
	case <-finch:
		return errors.New("closed")
	}
	return nil
}

// ResponseError for gen-server design pattern. Return err is not nil, else
// type-cast idx-th element in response to error and return the same.
func ResponseError(err error, response []interface{}, idx int) error {
	if err != nil {
		return err
	} else if response != nil {
		if response[idx] != nil {
			return response[idx].(error)
		}
		return nil
	}
	return nil
}

// Bytes2str morph byte slice to a string without copying. Note that the
// source byte-slice should remain in scope as long as string is in scope.
func Bytes2str(bytes []byte) string {
	if bytes == nil {
		return ""
	}
	sl := (*reflect.SliceHeader)(unsafe.Pointer(&bytes))
	st := &reflect.StringHeader{Data: sl.Data, Len: sl.Len}
	return *(*string)(unsafe.Pointer(st))
}

// Str2bytes morph string to a byte-slice without copying. Note that the
// source string should remain in scope as long as byte-slice is in scope.
func Str2bytes(str string) []byte {
	if str == "" {
		return nil
	}
	st := (*reflect.StringHeader)(unsafe.Pointer(&str))
	sl := &reflect.SliceHeader{Data: st.Data, Len: st.Len, Cap: st.Len}
	return *(*[]byte)(unsafe.Pointer(sl))
}

// GetStacktrace return stack-trace in human readable format.
func GetStacktrace(skip int, stack []byte) string {
	var buf bytes.Buffer
	lines := strings.Split(string(stack), "\n")
	for _, call := range lines[skip*2:] {
		buf.WriteString(fmt.Sprintf("%s\n", call))
	}
	return buf.String()
}

// Fixbuffer will expand the buffer if its capacity is less than size and
// return the buffer of size length.
func Fixbuffer(buffer []byte, size int64) []byte {
	if buffer == nil || int64(cap(buffer)) < size {
		buffer = make([]byte, size)
	}
	return buffer[:size]
}

// Prettystats uses json.MarshalIndent, if pretty is true, instead of
// json.Marshal. If Marshal return error Prettystats will panic.
func Prettystats(stats map[string]interface{}, pretty bool) string {
	if pretty {
		data, err := json.MarshalIndent(stats, "", "  ")
		if err != nil {
			panic(err)
		}
		return string(data)
	}
	data, err := json.Marshal(stats)
	if err != nil {
		panic(err)
	}
	return string(data)
}

// AbsInt64 absolute value of int64 number. Except for -2^63, where
// returned value will be same as input.
func AbsInt64(x int64) int64 {
	if x < 0 {
		return -x
	}
	return x
}
