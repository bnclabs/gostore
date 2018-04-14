package lsm

import "io"

type eofentry struct {
	key   []byte
	value []byte
	err   error
}

func neweofentry() *eofentry {
	eof := &eofentry{
		key: make([]byte, 0, 16), value: make([]byte, 0, 16), err: io.EOF,
	}
	return eof
}

func (entry *eofentry) Key() (key []byte, seqno uint64, del bool, err error) {
	key, err = entry.key, entry.err
	return
}

func (entry *eofentry) Value() (value []byte) {
	return entry.value
}

func (entry *eofentry) ID() string {
	return "--eof--"
}

func (entry *eofentry) Valueref() (valuelen uint64, vlogpos int64) {
	return 0, -1
}
