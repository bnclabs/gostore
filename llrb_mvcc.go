package storage

import "sync/atomic"
import "unsafe"

//---- LLRB MVCC write operations.

// snapshotting

type LLRBSnapshot struct {
	llrb     *LLRB
	root     unsafe.Pointer
	reclaim  []*Llrbnode
	next     unsafe.Pointer // *LLRBSnapshot
	refcount int32
}

func (llrb *LLRB) NewSnapshot() *LLRBSnapshot {
	location := &llrb.mvcc.readerhd
	reference := atomic.LoadPointer(location)
	for reference != nil {
		location = &((*LLRBSnapshot)(reference).next)
		reference = atomic.LoadPointer(location)
	}
	snapshot := &LLRBSnapshot{
		llrb: llrb,
		root: atomic.LoadPointer(&llrb.root),
	}
	if len(llrb.mvcc.cowednodes) > 0 {
		snapshot.reclaim = make([]*Llrbnode, len(llrb.mvcc.cowednodes))
	}
	copy(snapshot.reclaim, llrb.mvcc.cowednodes)
	llrb.mvcc.cowednodes = llrb.mvcc.cowednodes[:0]
	atomic.StorePointer(location, unsafe.Pointer(snapshot))
	return snapshot
}

func (snapshot *LLRBSnapshot) RefCount() {
	atomic.AddInt32(&snapshot.refcount, 1)
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
