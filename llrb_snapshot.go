// LLRB MVCC snapshot readers.

package storage

import "sync/atomic"
import "time"
import "strings"
import "io"
import "fmt"
import "strconv"
import "runtime/debug"

//---- snapshot ticker

func (writer *LLRBWriter) snapshotticker(interval int, finch chan bool) {
	llrb := writer.llrb
	tick := time.NewTicker(time.Duration(interval) * time.Millisecond)

	defer func() {
		if r := recover(); r != nil {
			log.Errorf("%v snapshotticker() crashed: %v\n", llrb.logPrefix, r)
			log.Errorf("\n%s", getStackTrace(2, debug.Stack()))
			llrb.Destroy()
		}
		tick.Stop()
	}()

loop:
	for {
		<-tick.C
		id := strconv.Itoa(int(time.Now().UnixNano() >> 19))
		select { // break out if writer has exited
		case <-finch:
			break loop
		default:
		}
		log.Tracef("%v snapshot tick for %v ...\n", llrb.logPrefix, id)
		if err := writer.makeSnapshot(id); err != nil {
			log.Errorf("%v make snapshot $%v failed: %v\n", llrb.logPrefix, err)
			break loop
		}
	}
}

// LLRBSnapshot holds on to a read-only version of the LLRB tree.
type LLRBSnapshot struct {
	llrb  *LLRB
	id    string
	root  *Llrbnode
	dead  bool
	clock *vectorclock

	// snapshot specific fields
	reclaim  []*Llrbnode
	next     *LLRBSnapshot
	refcount int64

	// config
	fmask     metadataMask
	logPrefix string

	// reader statistics
	n_lookups   int64
	n_ranges    int64
	n_cclookups int64
	n_ccranges  int64

	// writer statistics
	n_count       int64
	n_inserts     int64
	n_updates     int64
	n_deletes     int64
	n_allocs      int64
	n_frees       int64
	n_clones      int64
	keymemory     int64
	valmemory     int64
	h_upsertdepth histogramInt64
}

// NewSnapshot mvcc version for LLRB tree.
func (llrb *LLRB) NewSnapshot(id string) *LLRBSnapshot {
	snapshot := &LLRBSnapshot{
		llrb:  llrb,
		id:    id,
		root:  llrb.root,
		dead:  llrb.dead,
		clock: llrb.clock.clone(),
		fmask: llrb.fmask,
		// writer statistics
		n_count:       llrb.n_count,
		n_inserts:     llrb.n_inserts,
		n_updates:     llrb.n_updates,
		n_deletes:     llrb.n_deletes,
		n_allocs:      llrb.n_allocs,
		n_frees:       llrb.n_frees,
		n_clones:      llrb.n_clones,
		keymemory:     llrb.keymemory,
		valmemory:     llrb.valmemory,
		h_upsertdepth: *llrb.h_upsertdepth,
	}
	snapshot.logPrefix = fmt.Sprintf("[LLRBSnapshot-%s/%s]", llrb.name, id)

	snapshot.reclaim = make([]*Llrbnode, len(llrb.mvcc.reclaim))
	copy(snapshot.reclaim, llrb.mvcc.reclaim)
	llrb.mvcc.reclaim = llrb.mvcc.reclaim[:0] // reset writer reclaims

	// track to the tail of read-snapshot list.
	if llrb.mvcc.snapshot == nil {
		llrb.mvcc.snapshot = snapshot
	} else {
		parent := llrb.mvcc.snapshot
		for parent.next != nil {
			parent = parent.next
		}
		parent.next = snapshot
	}

	fmsg := "%v snapshot BORN %v nodes to reclaim...\n"
	log.Debugf(fmsg, snapshot.logPrefix, len(snapshot.reclaim))
	llrb.mvcc.n_snapshots += 1
	return snapshot
}

//---- Snapshot{} interface.

// Id implement Snapshot{} interface.
func (snapshot *LLRBSnapshot) Id() string {
	return snapshot.id
}

// Count implement Snapshot{} interface.
func (snapshot *LLRBSnapshot) Count() int64 {
	return snapshot.n_count
}

// Isactive implement Snapshot{} interface.
func (snapshot *LLRBSnapshot) Isactive() bool {
	return snapshot.dead == false
}

// Refer implement Snapshot interface.
func (snapshot *LLRBSnapshot) Refer() {
	log.Debugf("%v snapshot REF\n", snapshot.logPrefix)
	atomic.AddInt64(&snapshot.refcount, 1)
}

// Release implement Snapshot interface.
func (snapshot *LLRBSnapshot) Release() {
	log.Debugf("%v snapshot DEREF\n", snapshot.logPrefix)
	refcount := atomic.AddInt64(&snapshot.refcount, -1)
	if refcount < 0 {
		panic("Release(): snapshot refcount gone negative")
	}
}

// Validate implement Snapshot interface.
func (snapshot *LLRBSnapshot) Validate() {
	snapshot.llrb.validate(snapshot.root)
}

func (snapshot *LLRBSnapshot) Dotdump(buffer io.Writer) {
	lines := []string{
		"digraph llrb {",
		"  node[shape=record];",
		"}",
	}
	buffer.Write([]byte(strings.Join(lines[:len(lines)-1], "\n")))
	nd := snapshot.root
	nd.dotdump(buffer)
	buffer.Write([]byte(lines[len(lines)-1]))
}

//---- Reader{} interface.

// Has implement Reader{} interface.
func (snapshot *LLRBSnapshot) Has(key []byte) bool {
	return snapshot.Get(key) != nil
}

// Get implement Reader{} interface.
func (snapshot *LLRBSnapshot) Get(key []byte) Node {
	defer func() {
		if atomic.LoadInt64(&snapshot.llrb.mvcc.ismut) == 1 {
			atomic.AddInt64(&snapshot.n_cclookups, 1)
		} else {
			atomic.AddInt64(&snapshot.n_lookups, 1)
		}
	}()

	nd := snapshot.root
	for nd != nil {
		if nd.gtkey(key) {
			nd = nd.left
		} else if nd.ltkey(key) {
			nd = nd.right
		} else {
			return nd
		}
	}
	return nil // key is not present in the tree
}

// Min implement Reader{} interface.
func (snapshot *LLRBSnapshot) Min() Node {
	defer func() {
		if atomic.LoadInt64(&snapshot.llrb.mvcc.ismut) == 1 {
			atomic.AddInt64(&snapshot.n_cclookups, 1)
		} else {
			atomic.AddInt64(&snapshot.n_lookups, 1)
		}
	}()

	var nd *Llrbnode
	if nd = snapshot.root; nd == nil {
		return nil
	}
	for nd.left != nil {
		nd = nd.left
	}
	return nd
}

// Max implement Reader{} interface.
func (snapshot *LLRBSnapshot) Max() Node {
	defer func() {
		if atomic.LoadInt64(&snapshot.llrb.mvcc.ismut) == 1 {
			atomic.AddInt64(&snapshot.n_cclookups, 1)
		} else {
			atomic.AddInt64(&snapshot.n_lookups, 1)
		}
	}()

	var nd *Llrbnode
	if nd = snapshot.root; nd == nil {
		return nil
	}
	for nd.right != nil {
		nd = nd.right
	}
	return nd
}

// Range implement Reader{} interface.
func (s *LLRBSnapshot) Range(lkey, hkey []byte, incl string, iter NodeIterator) {
	defer func() {
		if atomic.LoadInt64(&s.llrb.mvcc.ismut) == 1 {
			atomic.AddInt64(&s.n_ccranges, 1)
		} else {
			atomic.AddInt64(&s.n_ranges, 1)
		}
	}()

	nd := s.root
	switch incl {
	case "both":
		s.llrb.rangeFromFind(nd, lkey, hkey, iter)
	case "high":
		s.llrb.rangeAfterFind(nd, lkey, hkey, iter)
	case "low":
		s.llrb.rangeFromTill(nd, lkey, hkey, iter)
	default:
		s.llrb.rangeAfterTill(nd, lkey, hkey, iter)
	}
}
