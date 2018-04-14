package llrb

import "github.com/bnclabs/gostore/lib"

type indexentry struct {
	id      string
	key     []byte
	value   []byte
	seqno   uint64
	deleted bool
	err     error
}

func (entry *indexentry) set(
	key, value []byte, seqno uint64, deleted bool, err error) *indexentry {

	entry.key = lib.Fixbuffer(entry.key, int64(len(key)))
	copy(entry.key, key)
	entry.value = lib.Fixbuffer(entry.value, int64(len(value)))
	copy(entry.value, value)

	entry.seqno, entry.deleted, entry.err = seqno, deleted, err
	return entry
}

func (entry *indexentry) Key() (key []byte, seqno uint64, del bool, err error) {
	return entry.key, entry.seqno, entry.deleted, entry.err
}

func (entry *indexentry) Value() (value []byte) {
	return entry.value
}

func (entry *indexentry) ID() string {
	return entry.id
}

func (entry *indexentry) Valueref() (valuelen uint64, vlogpos int64) {
	return uint64(len(entry.value)), -1
}
