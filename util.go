package storage

import "unsafe"
import "reflect"
import "fmt"
import "errors"
import "bytes"
import "strings"

var lookupffs = [256]int8{
	-1, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 4, 0, 1, 0, 2, 0, 1, 0,
	3, 0, 1, 0, 2, 0, 1, 0, 5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
	4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 6, 0, 1, 0, 2, 0, 1, 0,
	3, 0, 1, 0, 2, 0, 1, 0, 4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
	5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 4, 0, 1, 0, 2, 0, 1, 0,
	3, 0, 1, 0, 2, 0, 1, 0, 7, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
	4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 5, 0, 1, 0, 2, 0, 1, 0,
	3, 0, 1, 0, 2, 0, 1, 0, 4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
	6, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0, 4, 0, 1, 0, 2, 0, 1, 0,
	3, 0, 1, 0, 2, 0, 1, 0, 5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
	4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
}

func findfirstset8(b byte) int8 { // move this to ASM.
	return lookupffs[b]
}

func clearbit8(b byte, n uint8) byte { // move this to ASM.
	return b & (0xff ^ (1 << n))
}

func setbit8(b byte, n uint8) byte { // move this to ASM.
	return b | (1 << n)
}

func onesin8(v byte) (c int8) { // move this to ASM.
	for c = 0; v != 0; v >>= 1 { // count set bits
		c += int8(v & 1)
	}
	return c
}

func zerosin8(v byte) int8 { // move this to ASM.
	return 8 - onesin8(v)
}

func onesin32(v uint32) int8 { // move this to ASM.
	v = v - ((v >> 1) & 0x55555555)
	v = (v & 0x33333333) + ((v >> 2) & 0x33333333)
	return int8((((v + (v >> 4)) & 0x0F0F0F0F) * 0x01010101) >> 24)
}

func zerosin32(v uint32) int8 {
	return 32 - onesin32(v)
}

func memcpy(dst, src unsafe.Pointer, ln int) int {
	var srcnd, dstnd []byte
	srcsl := (*reflect.SliceHeader)(unsafe.Pointer(&srcnd))
	srcsl.Len, srcsl.Cap = ln, ln
	srcsl.Data = (uintptr)(unsafe.Pointer(src))
	dstsl := (*reflect.SliceHeader)(unsafe.Pointer(&dstnd))
	dstsl.Len, dstsl.Cap = ln, ln
	dstsl.Data = (uintptr)(unsafe.Pointer(dst))
	return copy(dstnd, srcnd)
}

func failsafeRequest(
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

func failsafePost(
	reqch chan []interface{}, cmd []interface{}, finch chan bool) error {

	select {
	case reqch <- cmd:
	case <-finch:
		return errors.New("closed")
	}
	return nil
}

func responseError(err error, resp []interface{}, idx int) error {
	if err != nil {
		return err
	} else if resp != nil {
		if resp[idx] != nil {
			return resp[idx].(error)
		} else {
			return nil
		}
	}
	return nil
}

func bytes2str(bytes []byte) string {
	if bytes == nil {
		return ""
	}
	sl := (*reflect.SliceHeader)(unsafe.Pointer(&bytes))
	st := &reflect.StringHeader{Data: sl.Data, Len: sl.Len}
	return *(*string)(unsafe.Pointer(st))
}

func str2bytes(str string) []byte {
	if str == "" {
		return nil
	}
	st := (*reflect.StringHeader)(unsafe.Pointer(&str))
	sl := &reflect.SliceHeader{Data: st.Data, Len: st.Len, Cap: st.Len}
	return *(*[]byte)(unsafe.Pointer(sl))
}

func llndornil(nd *Llrbnode) Node {
	if nd == nil {
		return nil
	}
	return nd
}

func getStackTrace(skip int, stack []byte) string {
	var buf bytes.Buffer
	lines := strings.Split(string(stack), "\n")
	for _, call := range lines[skip*2:] {
		buf.WriteString(fmt.Sprintf("%s\n", call))
	}
	return buf.String()
}
