package llrb

import "io"
import "fmt"
import "unsafe"
import "strings"
import "strconv"
import "sync/atomic"

import "github.com/prataprc/gostore/lib"

// mvccsnapshot refers to MVCC snapshot of LLRB tree. Snapshots
// can be used for concurrent reads.
type mvccsnapshot struct {
	mvcc      *MVCC
	root      *Llrbnode
	next      *mvccsnapshot
	reclaims  []*Llrbnode
	refcount  int64
	n_count   int64
	logprefix []byte
}

func (snap *mvccsnapshot) initsnapshot(mvcc *MVCC) *mvccsnapshot {
	snap.mvcc, snap.root = mvcc, (*Llrbnode)(mvcc.root)
	snap.next = (*mvccsnapshot)(mvcc.snapshot)
	snap.refcount, snap.n_count = 0, mvcc.Count()

	if cap(snap.reclaims) < len(mvcc.reclaims) {
		snap.reclaims = make([]*Llrbnode, len(mvcc.reclaims))
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

// ID return unique id for snapshot.
func (snap *mvccsnapshot) id() string {
	return fmt.Sprintf("%p", snap.root)
}

// Dotdump to convert whole tree into dot script that can be
// visualized using graphviz.
func (snap *mvccsnapshot) dotdump(buffer io.Writer) {
	lines := []string{
		"digraph llrb {",
		"  node[shape=record];\n",
		"}",
	}
	buffer.Write([]byte(strings.Join(lines[:len(lines)-1], "\n")))
	snap.root.dotdump(buffer)
	buffer.Write([]byte(lines[len(lines)-1]))
}

// Count return the number of items indexed in snapshot.
func (snap *mvccsnapshot) count() int64 {
	return snap.n_count
}

//---- Exported Read methods

// Get value for key, if value argument is not nil it will be used to copy the
// entry's value. Also returns entry's cas, whether entry is marked as deleted
// by LSM. If ok is false, then key is not found.
func (snap *mvccsnapshot) get(
	key, value []byte) (v []byte, cas uint64, deleted bool, ok bool) {

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

func (snap *mvccsnapshot) getkey(nd *Llrbnode, k []byte) (*Llrbnode, bool) {
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

func (snap *mvccsnapshot) getref() int64 {
	return atomic.LoadInt64(&snap.refcount)
}

func (snap *mvccsnapshot) refer() int64 {
	return atomic.AddInt64(&snap.refcount, 1)
}

func (snap *mvccsnapshot) release() int64 {
	refcount := atomic.AddInt64(&snap.refcount, -1)
	snap.mvcc.releasesnapshot(snap.next)
	return refcount
}

func (snap *mvccsnapshot) abortview(view *View) {
	snap.mvcc.abortview(view)
}
