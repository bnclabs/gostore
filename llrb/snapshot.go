package llrb

import "io"
import "fmt"
import "unsafe"
import "strings"
import "sync/atomic"

import "github.com/prataprc/gostore/lib"

// mvccsnapshot refers to MVCC snapshot of LLRB tree. Snapshots
// can be used for concurrent reads.
type mvccsnapshot struct {
	// must be 9-byte aligned.
	refcount int64
	purgetry int64
	n_count  int64
	seqno    uint64

	id       int64
	mvcc     *MVCC
	root     unsafe.Pointer // *Llrbnode
	next     unsafe.Pointer // *mvccsnapshot
	reclaims []*Llrbnode
	reclaim  []*Llrbnode
}

// Should be under write-lock.
func (snap *mvccsnapshot) initsnapshot(
	id int64, mvcc *MVCC, head *mvccsnapshot) *mvccsnapshot {

	snap.mvcc, snap.root = mvcc, nil
	atomic.StorePointer(&snap.next, unsafe.Pointer(head))
	// IMPORTANT: don't update refcount and n_count atomically here.
	// it can catch bugs in purging and re-cycling snapshots.
	snap.refcount = 0
	snap.n_count = 0
	if head != nil {
		snap.root = atomic.LoadPointer(&head.root)
		snap.n_count = mvcc.Count()
		//fmt.Printf("initsnapshot %v %v\n", time.Now(), len(head.reclaims))
	}
	snap.reclaims, snap.reclaim = snap.reclaims[:0], snap.reclaim[:0]
	atomic.StoreInt64(&snap.id, id)
	return snap
}

//---- Exported Control methods

// Dotdump to convert whole tree into dot script that can be
// visualized using graphviz.
func (snap *mvccsnapshot) dotdump(buffer io.Writer) {
	lines := []string{
		"digraph llrb {",
		"  node[shape=record];\n",
		"}",
	}
	buffer.Write([]byte(strings.Join(lines[:len(lines)-1], "\n")))
	root := snap.getroot()
	root.dotdump(buffer)
	buffer.Write([]byte(lines[len(lines)-1]))
}

// return the number of items indexed in snapshot.
func (snap *mvccsnapshot) count() int64 {
	return atomic.LoadInt64(&snap.n_count)
}

func (snap *mvccsnapshot) getroot() *Llrbnode {
	return (*Llrbnode)(atomic.LoadPointer(&snap.root))
}

func (snap *mvccsnapshot) setroot(root *Llrbnode) {
	atomic.StorePointer(&snap.root, unsafe.Pointer(root))
}

//---- Exported Read methods

// Get value for key, if value argument is not nil it will be used to copy the
// entry's value. Also returns entry's cas, whether entry is marked as deleted
// by LSM. If ok is false, then key is not found.
func (snap *mvccsnapshot) get(
	key, value []byte) (v []byte, cas uint64, deleted bool, ok bool) {

	deleted, seqno := false, uint64(0)
	nd, ok := snap.getkey(snap.getroot(), key)
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
	if refcount < 0 {
		panic(fmt.Errorf("unexpected refcount %v", refcount))
	}
	return refcount
}

func (snap *mvccsnapshot) abortview(view *View) {
	snap.mvcc.abortview(view)
}

func (snap *mvccsnapshot) trypurge() {
	atomic.StoreInt64(&snap.purgetry, 1)
}

func (snap *mvccsnapshot) clearpurge() {
	atomic.StoreInt64(&snap.purgetry, 0)
}

func (snap *mvccsnapshot) istrypurge() bool {
	if atomic.LoadInt64(&snap.purgetry) > 0 {
		return true
	}
	return false
}
