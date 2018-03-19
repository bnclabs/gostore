package bubt

import "fmt"

import "github.com/bnclabs/gostore/lib"

type lazyvalue struct {
	actual   []byte
	shardidx int
	fpos     int64
	valuelen int64
}

func (lv *lazyvalue) setfields(valuelen, vlogpos int64, value []byte) {
	if len(value) > 0 {
		lv.actual = value
	} else {
		lv.actual = nil
	}
	lv.valuelen = valuelen
	lv.shardidx = int(uint64(vlogpos) >> 56)
	lv.fpos = int64(uint64(vlogpos) & 0xFF00000000000000)
}

func (lv *lazyvalue) getactual(snap *Snapshot, vblock []byte) ([]byte, []byte) {
	if len(lv.actual) > 0 {
		return lv.actual, vblock

	} else if lv.valuelen == 0 {
		return nil, vblock
	}

	vblock = lib.Fixbuffer(vblock, lv.valuelen)
	r := snap.readvs[lv.shardidx-1]
	n, err := r.ReadAt(vblock, lv.fpos)
	if err != nil {
		panic(err)

	} else if n < len(vblock) {
		err := fmt.Errorf("bubt.snap.partialvlog %v < %v", n, len(vblock))
		panic(err)
	}
	return vblock[8:], vblock
}

func (lv *lazyvalue) inlinevalue() []byte {
	return lv.actual
}
