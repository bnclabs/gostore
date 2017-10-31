package bogn

import "github.com/prataprc/gostore/api"

func init() {
	// check whether bogn confirms to api.Index{} interface.
	var _ api.Index = &Bogn{}
}
