package api

import "bytes"
import "reflect"
import "unsafe"

// Binarycmp is same as bytes.Compare expect for partial matches. For EG:
// bytes.Compare([]byte("aaa"), []byte("aa")) shall return 1, whereas
// Binarycmp will return 0. Note that partial comparison is applicable only
// for limit. That is, Binarycmp([]byte("aa"), []byte("aaa")) will return -1,
// same as bytes.Compare.
func Binarycmp(key, limit []byte, partial bool) int {
	if ln := len(limit); partial && ln < len(key) {
		return bytes.Compare(key[:ln], limit[:ln])
	}
	return bytes.Compare(key, limit)
}

// Fixbuffer will expand the buffer, if its capacity is less than size and
// return the buffer of size length.
func Fixbuffer(buffer []byte, size int64) []byte {
	if buffer == nil || int64(cap(buffer)) < size {
		buffer = make([]byte, size)
	}
	return buffer[:size]
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
