package llrb

import "io"
import "fmt"
import "unsafe"
import "sync/atomic"
import "strconv"

type Snapshot struct {
	mvcc      *MVCC
	root      *Llrbnode1
	next      *Snapshot
	reclaims  []*Llrbnode1
	refcount  int64
	logprefix []byte
}

func (snap *Snapshot) initsnapshot(mvcc *MVCC) *Snapshot {
	snap.mvcc, snap.root = mvcc, (*Llrbnode1)(mvcc.root)
	snap.next = (*Snapshot)(mvcc.snapshot)
	snap.refcount = 0

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
}

func (snap *Snapshot) Clone(name string) *MVCC {
	return nil
}

func (snap *Snapshot) Count() int64 {
	return 0
}

func (snap *Snapshot) Log() {
}

func (snap *Snapshot) Stats() map[string]interface{} {
	return nil
}

func (snap *Snapshot) Validate() {
}

//---- Exported Read methods

func (snap *Snapshot) Get(key, value []byte) ([]byte, uint64, bool, bool) {
	return nil, 0, false, false
}

//---- local methods

func (snap *Snapshot) refer() int64 {
	return atomic.AddInt64(&snap.refcount, 1)
}

func (snap *Snapshot) release() int64 {
	refcount := atomic.AddInt64(&snap.refcount, -1)
	snap.mvcc.releasesnapshot(snap.next)
	return refcount
}
