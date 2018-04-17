package bogn

import "github.com/bnclabs/gostore/api"

type disksnap struct {
	bogn *Bogn
	disk api.Index
}

func (d *disksnap) Appdata() []byte {
	return d.bogn.getappdata(d.disk)
}
