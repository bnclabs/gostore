package llrb

import "io"
import "fmt"
import "unsafe"
import "strings"
import "strconv"
import "sync/atomic"

import "github.com/prataprc/gostore/lib"

type Snapshot struct {
	mvcc      *MVCC
	root      *Llrbnode1
	next      *Snapshot
	reclaims  []*Llrbnode1
	refcount  int64
	n_count   int64
	logprefix []byte
}

func (snap *Snapshot) initsnapshot(mvcc *MVCC) *Snapshot {
	snap.mvcc, snap.root = mvcc, (*Llrbnode1)(mvcc.root)
	snap.next = (*Snapshot)(mvcc.snapshot)
	snap.refcount, snap.n_count = 0, mvcc.Count()

	if cap(snap.reclaims) < len(mvcc.reclaims) {
		snap.reclaims = make([]*Llrbnode1, len(mvcc.reclaims))
	}
	copy(snap.reclaims, mvcc.reclaims)

	if snap.logprefix == nil {
		snap.logprefix = make([]byte, 64)
	}
	ptr := (uintptr)(unsafe.Pointer(snap.root))
	snap.logprefix = strconv.AppendUint(snap.logprefix, (uint64)(ptr), 16)

	mvcc.snapshot = unsafe.Pointer(snap)
	return snap
}

//---- Exported Control methods

func (snap *Snapshot) ID() string {
	return fmt.Sprintf("%p", snap.root)
}

func (snap *Snapshot) Dotdump(buffer io.Writer) {
	lines := []string{
		"digraph llrb {",
		"  node[shape=record];\n",
		"}",
	}
	buffer.Write([]byte(strings.Join(lines[:len(lines)-1], "\n")))
	snap.root.dotdump(buffer)
	buffer.Write([]byte(lines[len(lines)-1]))
}

func (snap *Snapshot) Count() int64 {
	return snap.n_count
}

//---- Exported Read methods

func (snap *Snapshot) Get(key, value []byte) ([]byte, uint64, bool, bool) {
	deleted, seqno := false, uint64(0)
	nd, ok := snap.getkey(snap.root, key)
	if ok {
		if value != nil {
			val := nd.Value()
			value = lib.Fixbuffer(value, int64(len(val)))
			copy(value, val)
		}
		seqno, deleted = nd.getseqno(), nd.isdeleted()
	} else if value != nil {
		value = lib.Fixbuffer(value, 0)
	}
	return value, seqno, deleted, ok
}

func (snap *Snapshot) getkey(nd *Llrbnode1, k []byte) (*Llrbnode1, bool) {
	for nd != nil {
		if nd.gtkey(k, false) {
			nd = nd.left
		} else if nd.ltkey(k, false) {
			nd = nd.right
		} else {
			return nd, true
		}
	}
	return nil, false
}

//---- local methods

func (snap *Snapshot) getref() int64 {
	return atomic.LoadInt64(&snap.refcount)
}

func (snap *Snapshot) refer() int64 {
	return atomic.AddInt64(&snap.refcount, 1)
}

func (snap *Snapshot) release() int64 {
	refcount := atomic.AddInt64(&snap.refcount, -1)
	snap.mvcc.releasesnapshot(snap.next)
	return refcount
}
