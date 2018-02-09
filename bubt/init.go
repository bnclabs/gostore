package bubt

import "github.com/bnclabs/gostore/api"

func init() {
	// check whether Snapshot implement api.Index
	var _ api.Index = &Snapshot{}
}
