// LLRB MVCC snapshot readers.

package storage

import "sync/atomic"
import "unsafe"
import "time"
import "strings"
import "io"
import "strconv"

//---- snapshot methods

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
		if err := writer.MakeSnapshot(id); err != nil {
			log.Errorf("%v make snapshot $%v failed: %v\n", llrb.logPrefix, err)
			break loop
		}
		log.Debugf("%v scheduled a new snapshot $%v\n", llrb.logPrefix, id)
	}
}

// read-snapshots

type LLRBSnapshot struct {
	llrb     *LLRB
	id       string
	clock    *vectorclock
	root     unsafe.Pointer
	reclaim  []*Llrbnode
	next     unsafe.Pointer // *LLRBSnapshot
	refcount int32
}

func (llrb *LLRB) NewSnapshot(id string) *LLRBSnapshot {
	// track to the tail of read-snapshot list.
	location := &llrb.mvcc.snapshot
	upsnapshot := atomic.LoadPointer(location)
	for upsnapshot != nil {
		location = &((*LLRBSnapshot)(upsnapshot).next)
		upsnapshot = atomic.LoadPointer(location)
	}
	snapshot := &LLRBSnapshot{
		llrb:  llrb,
		id:    id,
		root:  atomic.LoadPointer(&llrb.root),
		clock: llrb.clock.clone(),
	}

	snapshot.reclaim = make([]*Llrbnode, len(llrb.mvcc.reclaim))
	copy(snapshot.reclaim, llrb.mvcc.reclaim)
	llrb.mvcc.reclaim = llrb.mvcc.reclaim[:0] // reset writer reclaims

	atomic.StorePointer(location, unsafe.Pointer(snapshot))

	fmsg := "%v new snapshot $%v with %v nodes to reclaim...\n"
	log.Debugf(fmsg, llrb.logPrefix, id, len(snapshot.reclaim))
	return snapshot
}

func (snapshot *LLRBSnapshot) Id() string {
	return snapshot.id
}

func (snapshot *LLRBSnapshot) Release() {
	atomic.AddInt32(&snapshot.refcount, -1)
	log.Debugf("%v deref snapshot %v\n", snapshot.llrb.logPrefix, snapshot.id)
}

func (snapshot *LLRBSnapshot) ReclaimNodes() bool {
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

func (snapshot *LLRBSnapshot) Has(key []byte) bool {
	return snapshot.Get(key) != nil
}

func (snapshot *LLRBSnapshot) Get(key []byte) (nd *Llrbnode) {
	nd = (*Llrbnode)(atomic.LoadPointer(&snapshot.root))
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

func (snapshot *LLRBSnapshot) Min() (nd *Llrbnode) {
	if nd = (*Llrbnode)(atomic.LoadPointer(&snapshot.root)); nd == nil {
		return nil
	}

	for nd.left != nil {
		nd = nd.left
	}
	return nd
}

func (snapshot *LLRBSnapshot) Max() (nd *Llrbnode) {
	if nd = (*Llrbnode)(atomic.LoadPointer(&snapshot.root)); nd == nil {
		return nil
	}

	for nd.right != nil {
		nd = nd.right
	}
	return nd
}

// Range from lkey to hkey, incl can be "both", "low", "high", "none"
func (snapshot *LLRBSnapshot) Range(lkey, hkey []byte, incl string, iter LLRBNodeIterator) {
	if iter == nil {
		panic("Range(): iter argument is nil")
	}

	nd := (*Llrbnode)(atomic.LoadPointer(&snapshot.root))
	switch incl {
	case "both":
		snapshot.llrb.rangeFromFind(nd, lkey, hkey, iter)
	case "high":
		snapshot.llrb.rangeAfterFind(nd, lkey, hkey, iter)
	case "low":
		snapshot.llrb.rangeFromTill(nd, lkey, hkey, iter)
	default:
		snapshot.llrb.rangeAfterTill(nd, lkey, hkey, iter)
	}
}

func (snapshot *LLRBSnapshot) ValidateReds() bool {
	root := (*Llrbnode)(atomic.LoadPointer(&snapshot.root))
	if snapshot.llrb.validatereds(root, isred(root)) != true {
		return false
	}
	return true
}

func (snapshot *LLRBSnapshot) ValidateBlacks() int {
	root := (*Llrbnode)(atomic.LoadPointer(&snapshot.root))
	return snapshot.llrb.validateblacks(root, 0)
}

func (snapshot *LLRBSnapshot) ValidateHeight() bool {
	root := (*Llrbnode)(atomic.LoadPointer(&snapshot.root))
	heightav := &averageInt{}
	return snapshot.llrb.validateheight(root, heightav)
}

func (snapshot *LLRBSnapshot) Dotdump(buffer io.Writer) {
	lines := []string{
		"digraph llrb {",
		"  node[shape=record];",
		"}",
	}
	buffer.Write([]byte(strings.Join(lines[:len(lines)-1], "\n")))
	nd := (*Llrbnode)(atomic.LoadPointer(&snapshot.root))
	nd.dotdump(buffer)
	buffer.Write([]byte(lines[len(lines)-1]))
}
