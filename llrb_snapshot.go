// LLRB MVCC snapshot readers.

package storage

import "sync/atomic"
import "unsafe"
import "time"
import "strings"
import "io"
import "fmt"
import "strconv"

//---- snapshot ticker

func (writer *LLRBWriter) snapshotticker(interval int, finch chan bool) {
	tick := time.NewTicker(time.Duration(interval) * time.Millisecond)
	defer func() {
		tick.Stop()
	}()

	llrb := writer.llrb

loop:
	for {
		<-tick.C
		id := strconv.Itoa(int(time.Now().UnixNano() >> 19))
		select { // break out if writer has exited
		case <-finch:
			break loop
		default:
		}
		if err := writer.makeSnapshot(id); err != nil {
			log.Errorf("%v make snapshot $%v failed: %v\n", llrb.logPrefix, err)
			break loop
		}
		log.Debugf("%v scheduled a new snapshot $%v\n", llrb.logPrefix, id)
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
	next     unsafe.Pointer // *LLRBSnapshot
	refcount int32

	// config
	fmask     metadataMask
	logPrefix string

	// statistics
	count       int64
	lookups     int64
	ranges      int64
	keymemory   int64
	valmemory   int64
	upsertdepth averageInt
}

// NewSnapshot mvcc version for LLRB tree.
func (llrb *LLRB) NewSnapshot(id string) *LLRBSnapshot {
	// track to the tail of read-snapshot list.
	location := &llrb.mvcc.snapshot
	upsnapshot := atomic.LoadPointer(location)
	for upsnapshot != nil {
		location = &((*LLRBSnapshot)(upsnapshot).next)
		upsnapshot = atomic.LoadPointer(location)
	}

	snapshot := &LLRBSnapshot{
		llrb:        llrb,
		id:          id,
		root:        llrb.root,
		dead:        llrb.dead,
		clock:       llrb.clock.clone(),
		fmask:       llrb.fmask,
		count:       llrb.count,
		keymemory:   llrb.keymemory,
		valmemory:   llrb.valmemory,
		upsertdepth: *llrb.upsertdepth,
	}
	llrb.logPrefix = fmt.Sprintf("[LLRBSnapshot-%s/%s]", llrb.name, id)

	snapshot.reclaim = make([]*Llrbnode, len(llrb.mvcc.reclaim))
	copy(snapshot.reclaim, llrb.mvcc.reclaim)
	llrb.mvcc.reclaim = llrb.mvcc.reclaim[:0] // reset writer reclaims

	atomic.StorePointer(location, unsafe.Pointer(snapshot))

	fmsg := "%v new snapshot $%v with %v nodes to reclaim...\n"
	log.Debugf(fmsg, llrb.logPrefix, id, len(snapshot.reclaim))
	return snapshot
}

//---- Snapshot{} interface.

// Id implement Snapshot{} interface.
func (snapshot *LLRBSnapshot) Id() string {
	return snapshot.id
}

// Count implement Snapshot{} interface.
func (snapshot *LLRBSnapshot) Count() int64 {
	return snapshot.count
}

// Isactive implement Snapshot{} interface.
func (snapshot *LLRBSnapshot) Isactive() bool {
	return snapshot.dead == false
}

// Release implement Snapshot interface.
func (snapshot *LLRBSnapshot) Release() {
	atomic.AddInt32(&snapshot.refcount, -1)
	log.Debugf("%v deref snapshot\n", snapshot.logPrefix)
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

//---- local methods.

func (snapshot *LLRBSnapshot) reclaimNodes() bool {
	if atomic.LoadInt32(&snapshot.refcount) < 0 {
		panic("snapshot refcount gone negative")
	} else if atomic.LoadInt32(&snapshot.refcount) == 0 {
		for _, nd := range snapshot.reclaim {
			snapshot.llrb.freenode(nd)
		}
		return true
	}
	return false
}
