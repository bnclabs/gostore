package api

import "bytes"
import "fmt"

var _ = fmt.Sprintf("dummy")

func Binarycmp(key, limit []byte, partial bool) int {
	if ln := len(limit); partial && ln < len(key) {
		return bytes.Compare(key[:ln], limit[:ln])
	}
	return bytes.Compare(key, limit)
}
