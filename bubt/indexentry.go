package bubt

import "github.com/bnclabs/gostore/lib"

type indexentry struct {
	id      string
	snap    *Snapshot
	key     []byte
	lv      lazyvalue
	seqno   uint64
	deleted bool
	err     error
	vblock  []byte
}

func (entry *indexentry) set(
	key []byte,
	lv lazyvalue, seqno uint64, deleted bool, err error) *indexentry {

	entry.key = lib.Fixbuffer(entry.key, int64(len(key)))
	copy(entry.key, key)
	entry.lv = lv

	entry.seqno, entry.deleted, entry.err = seqno, deleted, err
	return entry
}

func (entry *indexentry) Key() (key []byte, seqno uint64, del bool, err error) {
	return entry.key, entry.seqno, entry.deleted, entry.err
}

func (entry *indexentry) Value() (value []byte) {
	if entry.err != nil {
		return nil
	}
	value, entry.vblock = entry.lv.getactual(entry.snap, entry.vblock)
	return value
}

func (entry *indexentry) ID() string {
	return entry.id
}

func (entry *indexentry) Valueref() (valuelen uint64, vlogpos int64) {
	valuelen, vlogpos = uint64(entry.lv.valuelen), entry.lv.vlogpos
	return
}
