package bubt

import "github.com/prataprc/gostore/api"

func init() {
	// check whether Snapshot implement api.Index
	var _ api.Index = &Snapshot{}
}
