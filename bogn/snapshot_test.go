package bogn

import "testing"

import "github.com/bnclabs/gostore/api"

func BenchmarkNewsnapshot(b *testing.B) {
	var disks [16]api.Index

	bogn := &Bogn{}
	for i := 0; i < b.N; i++ {
		newsnapshot(bogn, nil, nil, nil, disks, "", 0)
	}
}
