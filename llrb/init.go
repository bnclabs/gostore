package llrb

import "github.com/prataprc/gostore/api"

func init() {
	// check whether llrb confirms to api.Index{} interface.
	var _ api.Index = &LLRB{}
	// check whether mvcc confirms to api.Index{} interface.
	var _ api.Index = &MVCC{}
}
