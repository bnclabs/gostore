package bogn

import "fmt"
import "unsafe"
import "sync/atomic"

import "github.com/prataprc/gostore/api"
import "github.com/prataprc/gostore/lib"
import "github.com/prataprc/gostore/lsm"
import "github.com/prataprc/gostore/llrb"
import "github.com/prataprc/golog"

type snapshot struct {
	refcount int64
	purgetry int64          // must be 8-byte aligned
	next     unsafe.Pointer // *snapshot

	id           string
	bogn         *Bogn
	mw, mr, mc   api.Index
	disks        [16]api.Index
	yget         api.Getter
	purgeindexes []api.Index

	setch   chan *setcache
	cachech chan *setcache
}

func opensnapshot(bogn *Bogn, disks [16]api.Index) (*snapshot, error) {
	var err error

	uuid := bogn.newuuid()

	head := &snapshot{id: uuid, bogn: bogn, disks: disks, next: nil}
	if head.mw, err = bogn.newmemstore("mw", 0); err != nil {
		return nil, err
	}
	if bogn.workingset {
		head.setch = make(chan *setcache, 1000)   // TODO: no magic number
		head.cachech = make(chan *setcache, 1000) // TODO: no magic number
		head.mc, err = bogn.newmemstore("mc", 0)
		if err != nil {
			return nil, err
		}
		go cacher(bogn, head.mc, head.setch, head.cachech)
	}
	head.yget = head.latestyget()
	log.Infof("%v open-snapshot %s", bogn.logprefix, head.id)
	return head, nil
}

func newsnapshot(
	bogn *Bogn, mw, mr, mc api.Index, disks [16]api.Index,
	uuid string) *snapshot {

	if uuid == "" {
		uuid = bogn.newuuid()
	}
	head := &snapshot{id: uuid, bogn: bogn, mw: mw, mr: mr, mc: mc}
	copy(head.disks[:], disks[:])
	if head.mc != nil {
		head.setch = make(chan *setcache, 1000)   // TODO: no magic number
		head.cachech = make(chan *setcache, 1000) // TODO: no magic number
		go cacher(bogn, head.mc, head.setch, head.cachech)
	}
	head.yget = head.latestyget()
	log.Infof("%v new-snapshot %s", bogn.logprefix, head.id)
	return head
}

func (snap *snapshot) mwseqno() uint64 {
	switch index := snap.mw.(type) {
	case *llrb.LLRB:
		return index.Getseqno()
	case *llrb.MVCC:
		return index.Getseqno()
	}
	panic("unreachable code")
}

func (snap *snapshot) addtopurge(indexes ...api.Index) {
	if len(snap.purgeindexes) > 0 {
		snap.purgeindexes = append(snap.purgeindexes, indexes...)
	} else {
		snap.purgeindexes = append([]api.Index{}, indexes...)
	}
}

func (snap *snapshot) latestlevel() (int, api.Index) {
	for level, disk := range snap.disks {
		if disk != nil {
			if lvl, _, _ := snap.bogn.path2level(disk.ID()); level != lvl {
				panic(fmt.Errorf("mismatch in level %v != %v", level, lvl))
			}
			return level, disk
		}
	}
	return -1, nil
}

func (snap *snapshot) nextemptylevel(level int) (nextlevel int) {
	if level >= (len(snap.disks) - 1) {
		return -1
	}
	nextlevel = -1
	for l, disk := range snap.disks[level+1:] {
		if disk == nil {
			nextlevel = level + 1 + l
		}
		return nextlevel
	}
	return nextlevel
}

func (snap *snapshot) oldestlevel() (int, api.Index) {
	for level := len(snap.disks) - 1; level >= 0; level-- {
		if disk := snap.disks[level]; disk != nil {
			if lvl, _, _ := snap.bogn.path2level(disk.ID()); level != lvl {
				panic(fmt.Errorf("mismatch in level %v != %v", level, lvl))
			}
			return level, disk
		}
	}
	return -1, nil
}

func (snap *snapshot) disklevels(disks []api.Index) []api.Index {
	for _, disk := range snap.disks {
		if disk != nil {
			disks = append(disks, disk)
		}
	}
	return disks
}

func (snap *snapshot) memheap() int64 {
	memindexes, heap := []api.Index{snap.mw, snap.mr, snap.mc}, int64(0)
	for _, memindex := range memindexes {
		switch v := memindex.(type) {
		case *llrb.LLRB:
			heap += v.Footprint()
		case *llrb.MVCC:
			heap += v.Footprint()
		}
	}
	return heap
}

func (snap *snapshot) latestyget() (get api.Getter) {
	gets := []api.Getter{}
	if snap.mw != nil {
		gets = append(gets, snap.mw.Get)
	}
	if snap.mr != nil {
		gets = append(gets, snap.mr.Get)
	}
	if snap.mc != nil {
		gets = append(gets, snap.mc.Get)
	}

	var dget api.Getter
	for _, disk := range snap.disklevels([]api.Index{}) {
		if dget != nil && snap.mc != nil {
			dget = snap.cachedget(disk.Get)
		} else {
			dget = disk.Get
		}
		gets = append(gets, dget)
	}

	if len(gets) == 0 {
		return nil
	}
	get = gets[len(gets)-1]
	for i := len(gets) - 2; i >= 0; i-- {
		get = lsm.YGet(get, gets[i])
	}
	return
}

func (snap *snapshot) txnyget(
	mwtxn api.Transactor, gets []api.Getter) (api.Getter, []api.Getter) {

	if gets == nil {
		gets = make([]api.Getter, 0, 8)
	}
	gets = gets[:0]

	if mwtxn != nil {
		gets = append(gets, mwtxn.Get)
	}
	if snap.mr != nil {
		gets = append(gets, snap.mr.Get)
	}
	if snap.mc != nil {
		gets = append(gets, snap.mc.Get)
	}

	var dget api.Getter
	for _, disk := range snap.disklevels([]api.Index{}) {
		if dget != nil && snap.mc != nil {
			dget = snap.cachedget(disk.Get)
		} else {
			dget = disk.Get
		}
		gets = append(gets, dget)
	}

	if len(gets) == 0 {
		return nil, gets
	}
	get := gets[len(gets)-1]
	for i := len(gets) - 2; i >= 0; i-- {
		get = lsm.YGet(get, gets[i])
	}
	return get, gets
}

// try caching the entry from this get operation.
func (snap *snapshot) cachedget(get api.Getter) api.Getter {
	return func(key, value []byte) ([]byte, uint64, bool, bool) {
		value, cas, deleted, ok := get(key, value)
		if ok == false {
			return value, cas, deleted, ok
		}

		// TODO: if `mc` is skip list with concurrent writes, could
		// be an optimized solution.
		select {
		case cmd := <-snap.cachech:
			cmd.key = lib.Fixbuffer(cmd.key, int64(len(key)))
			copy(cmd.key, key)
			cmd.value = lib.Fixbuffer(cmd.value, int64(len(value)))
			copy(cmd.value, value)
			cmd.seqno = cas
			cmd.deleted = deleted
			select {
			case snap.setch <- cmd:
			default:
			}

		default:
		}
		return value, cas, deleted, ok
	}
}

func (snap *snapshot) iterator() (scan api.Iterator) {
	var ref [20]api.Iterator
	scans := ref[:0]

	if iter := snap.mw.Scan(); iter != nil {
		scans = append(scans, iter)
	}
	if snap.mr != nil {
		if iter := snap.mr.Scan(); iter != nil {
			scans = append(scans, iter)
		}
	}
	for _, disk := range snap.disklevels([]api.Index{}) {
		if iter := disk.Scan(); iter != nil {
			scans = append(scans, iter)
		}
	}

	if len(scans) == 0 {
		return nil
	}
	scan = scans[len(scans)-1]
	for i := len(scans) - 2; i >= 0; i-- {
		scan = lsm.YSort(scan, scans[i])
	}
	return
}

// iterate on write store and disk store.
func (snap *snapshot) persistiterator() (scan api.Iterator) {
	var ref [20]api.Iterator
	scans := ref[:0]

	if iter := snap.mw.Scan(); iter != nil {
		scans = append(scans, iter)
	}
	for _, disk := range snap.disklevels([]api.Index{}) {
		if iter := disk.Scan(); iter != nil {
			scans = append(scans, iter)
		}
	}

	if len(scans) == 0 {
		return nil
	}
	scan = scans[len(scans)-1] // disk store.
	for i := len(scans) - 2; i >= 0; i-- {
		scan = lsm.YSort(scan, scans[i])
	}
	return
}

func (snap *snapshot) windupiterator(disk api.Index) (scan api.Iterator) {
	var ref [20]api.Iterator
	scans := ref[:0]

	if iter := snap.mw.Scan(); iter != nil {
		scans = append(scans, iter)
	}
	if disk != nil {
		if iter := disk.Scan(); iter != nil {
			scans = append(scans, iter)
		}
	}

	if len(scans) == 0 {
		return nil
	}
	scan = scans[len(scans)-1]
	for i := len(scans) - 2; i >= 0; i-- {
		scan = lsm.YSort(scan, scans[i])
	}
	return scan
}

// iterate on write store, read store, cache store and a latest disk store.
func (snap *snapshot) flushiterator(disk api.Index) (scan api.Iterator) {
	var ref [20]api.Iterator
	scans := ref[:0]

	if iter := snap.mr.Scan(); iter != nil {
		scans = append(scans, iter)
	}
	if snap.mc != nil {
		if iter := snap.mc.Scan(); iter != nil {
			scans = append(scans, iter)
		}
	}
	if disk != nil {
		if iter := disk.Scan(); iter != nil {
			scans = append(scans, iter)
		}
	}

	if len(scans) == 0 {
		return nil
	}
	scan = scans[len(scans)-1]
	for i := len(scans) - 2; i >= 0; i-- {
		scan = lsm.YSort(scan, scans[i])
	}
	return
}

func (snap *snapshot) compactiterator(d0, d1 api.Index) (scan api.Iterator) {
	var ref [20]api.Iterator
	scans := ref[:0]

	if d0 == nil {
		return d1.Scan()
	} else if d1 == nil {
		return d0.Scan()
	}

	if iter := d0.Scan(); iter != nil {
		scans = append(scans, iter)
	}
	if iter := d1.Scan(); iter != nil {
		scans = append(scans, iter)
	}

	if len(scans) == 0 {
		return nil
	}
	scan = scans[len(scans)-1]
	for i := len(scans) - 2; i >= 0; i-- {
		scan = lsm.YSort(scan, scans[i])
	}
	return
}

func (snap *snapshot) delete(key, value []byte, lsm bool) ([]byte, uint64) {
	return snap.mw.Delete(key, value, lsm)
}

func (snap *snapshot) set(key, value, oldvalue []byte) ([]byte, uint64) {
	return snap.mw.Set(key, value, oldvalue)
}

func (snap *snapshot) setCAS(
	key, value, oldvalue []byte, cas uint64) ([]byte, uint64, error) {
	return snap.mw.SetCAS(key, value, oldvalue, cas)
}

func (snap *snapshot) close() {
	if snap.bogn.workingset {
		close(snap.setch)
	}

	snap.bogn = nil
	snap.mw, snap.mr, snap.mc = nil, nil, nil
	for i := range snap.disks {
		snap.disks[i] = nil
	}
	snap.yget, snap.purgeindexes = nil, nil
	atomic.StorePointer(&snap.next, nil)
}

func (snap *snapshot) getref() int64 {
	return atomic.LoadInt64(&snap.refcount)
}

func (snap *snapshot) refer() int64 {
	return atomic.AddInt64(&snap.refcount, 1)
}

func (snap *snapshot) release() int64 {
	refcount := atomic.AddInt64(&snap.refcount, -1)
	return refcount
}

func (snap *snapshot) setpurge() {
	atomic.StoreInt64(&snap.purgetry, 1)
}

func (snap *snapshot) clearpurge() {
	atomic.StoreInt64(&snap.purgetry, 0)
}

func (snap *snapshot) istrypurge() bool {
	if atomic.LoadInt64(&snap.purgetry) > 0 {
		return true
	}
	return false
}
