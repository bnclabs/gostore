package lib

import "unsafe"
import "reflect"
import "fmt"
import "errors"
import "bytes"
import "strings"

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

// FailsafeRequest gen-server api abstraction.
func FailsafeRequest(
	reqch, respch chan []interface{},
	cmd []interface{}, finch chan bool) ([]interface{}, error) {

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

// FailsafePost gen-server api abstraction.
func FailsafePost(
	reqch chan []interface{}, cmd []interface{}, finch chan bool) error {

	select {
	case reqch <- cmd:
	case <-finch:
		return errors.New("closed")
	}
	return nil
}

// ResponseError gen-server api abstraction.
func ResponseError(err error, resp []interface{}, idx int) error {
	if err != nil {
		return err
	} else if resp != nil {
		if resp[idx] != nil {
			return resp[idx].(error)
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

func panicerr(fmsg string, args ...interface{}) {
	panic(fmt.Errorf(fmsg, args...))
}
