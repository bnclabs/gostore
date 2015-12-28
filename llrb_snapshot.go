package storage

import "sync/atomic"
import "unsafe"

//---- LLRB MVCC snapshot readers.

// snapshotting

type LLRBSnapshot struct {
	llrb     *LLRB
	clock    *vectorclock
	root     unsafe.Pointer
	reclaim  []*Llrbnode
	next     unsafe.Pointer // *LLRBSnapshot
	refcount int32
}

func (llrb *LLRB) NewSnapshot() *LLRBSnapshot {
	// track to the tail of read-snapshot list.
	location := &llrb.mvcc.snapshot
	upsnapshot := atomic.LoadPointer(location)
	for upsnapshot != nil {
		location = &((*LLRBSnapshot)(upsnapshot).next)
		upsnapshot = atomic.LoadPointer(location)
	}
	snapshot := &LLRBSnapshot{
		llrb:  llrb,
		root:  atomic.LoadPointer(&llrb.root),
		clock: llrb.clock.clone(),
	}

	if len(llrb.mvcc.reclaim) > 0 {
		snapshot.reclaim = make([]*Llrbnode, len(llrb.mvcc.reclaim))
	}
	copy(snapshot.reclaim, llrb.mvcc.reclaim)

	llrb.mvcc.reclaim = llrb.mvcc.reclaim[:0]

	atomic.StorePointer(location, unsafe.Pointer(snapshot))
	return snapshot
}

func (snapshot *LLRBSnapshot) Release() {
	atomic.AddInt32(&snapshot.refcount, -1)
	if atomic.LoadInt32(&snapshot.refcount) == 0 {
	}
}

func (snapshot *LLRBSnapshot) Destroy() {
	llrb := snapshot.llrb
	for _, nd := range snapshot.reclaim {
		llrb.freenode(nd)
	}
}

func (snapshot *LLRBSnapshot) ReclaimNodes() bool {
	if atomic.LoadInt32(&snapshot.refcount) == 0 {
		for _, nd := range snapshot.reclaim {
			snapshot.llrb.freenode(nd)
		}
		return true
	}
	return false
}
