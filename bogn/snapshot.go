package bogn

import "fmt"
import "unsafe"
import "strconv"
import "strings"
import "runtime"
import "sync/atomic"

import "github.com/bnclabs/gostore/api"
import "github.com/bnclabs/gostore/lib"
import "github.com/bnclabs/gostore/lsm"
import "github.com/bnclabs/gostore/llrb"

type snapshot struct {
	// must be 8-byte aligned.
	refcount int64
	purgetry int64
	next     unsafe.Pointer // *snapshot

	id           string
	bogn         *Bogn
	beginseqno   uint64
	mw, mr, mc   api.Index
	disks        [16]api.Index
	yget         api.Getter
	purgeindexes []api.Index

	// working memory
	setch   chan *setcache
	cachech chan *setcache
}

func opensnapshot(
	bogn *Bogn, mw api.Index, disks [16]api.Index,
	beginseqno uint64) (*snapshot, error) {

	var err error

	uuid := bogn.newuuid()
	head := &snapshot{
		id: uuid, bogn: bogn, beginseqno: beginseqno,
		mw: mw, disks: disks, next: nil,
	}

	fmsg := "%v open-snapshot %s %v"
	infof(fmsg, bogn.logprefix, head.id, head.attributes())

	if head.mw == nil {
		if head.mw, err = bogn.newmemstore("mw", 0); err != nil {
			return nil, err
		}
	}
	if bogn.workingset {
		numcpu := runtime.GOMAXPROCS(-1) * 100
		head.setch = make(chan *setcache, numcpu)
		head.cachech = make(chan *setcache, numcpu)
		if head.mc, err = bogn.newmemstore("mc", 0); err != nil {
			return nil, err
		}
		go cacher(bogn, head.mc, head.setch, head.cachech)
	}
	head.yget = head.latestyget()
	return head, nil
}

func newsnapshot(
	bogn *Bogn, mw, mr, mc api.Index, disks [16]api.Index,
	uuid string, beginseqno uint64) *snapshot {

	if uuid == "" {
		uuid = bogn.newuuid()
	}
	head := &snapshot{
		id: uuid, bogn: bogn, beginseqno: beginseqno,
		mw: mw, mr: mr, mc: mc,
	}
	copy(head.disks[:], disks[:])
	if head.mc != nil {
		numcpu := runtime.GOMAXPROCS(-1) * 100
		head.setch = make(chan *setcache, numcpu)
		head.cachech = make(chan *setcache, numcpu)
		go cacher(bogn, head.mc, head.setch, head.cachech)
	}
	head.yget = head.latestyget()
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
	if snap.purgeindexes == nil {
		snap.purgeindexes = []api.Index{}
	}
	for _, index := range indexes {
		if index != nil {
			snap.purgeindexes = append(snap.purgeindexes, index)
		}
	}
}

func (snap *snapshot) isdirty() bool {
	mwseqno := snap.mwseqno()
	if mwseqno < snap.beginseqno {
		panic("impossible case")
	} else if mwseqno == snap.beginseqno {
		return false
	}
	return true
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

func (snap *snapshot) nextbutlevel(level int) (nextlevel int) {
	if level >= len(snap.disks) {
		panic("impossible situation")
	} else if level == (len(snap.disks) - 1) {
		return -1
	}
	nextlevel = -1
	for l, disk := range snap.disks[level+1:] {
		if disk == nil {
			nextlevel = level + 1 + l
			continue
		}
		return nextlevel
	}
	return nextlevel
}

func (snap *snapshot) oldestlevel() (int, api.Index) {
	if oldest := len(snap.disks) - 1; snap.disks[oldest] != nil {
		return oldest, snap.disks[oldest]
	}
	return -1, nil
}

// return a valid set of disk levels with newest level in the beginning.
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
		if memindex != nil {
			switch v := memindex.(type) {
			case *llrb.LLRB:
				heap += v.Footprint()
			case *llrb.MVCC:
				heap += v.Footprint()
			}
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

	if atomic.LoadInt64(&snap.bogn.dgmstate) == 1 {
		for _, disk := range snap.disklevels([]api.Index{}) {
			if snap.mc != nil {
				gets = append(gets, snap.cachedget(disk.Get))
			} else {
				gets = append(gets, disk.Get)
			}
		}
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
	tv api.Transactor, gets []api.Getter) api.Getter {

	var disks [256]api.Index

	if tv != nil {
		gets = append(gets, tv.Get)
	}
	if snap.mr != nil {
		gets = append(gets, snap.mr.Get)
	}
	if snap.mc != nil {
		gets = append(gets, snap.mc.Get)
	}

	if atomic.LoadInt64(&snap.bogn.dgmstate) == 1 {
		for _, disk := range snap.disklevels(disks[:0]) {
			if snap.mc != nil {
				gets = append(gets, snap.cachedget(disk.Get))
			} else {
				gets = append(gets, disk.Get)
			}
		}
	}

	if len(gets) == 0 {
		return nil
	}
	get := gets[len(gets)-1]
	for i := len(gets) - 2; i >= 0; i-- {
		get = lsm.YGet(get, gets[i])
	}
	return get
}

// try caching the entry from this get operation.
func (snap *snapshot) cachedget(get api.Getter) api.Getter {
	return func(key, value []byte) ([]byte, uint64, bool, bool) {
		value, cas, deleted, ok := get(key, value)
		if ok == false {
			return value, cas, deleted, ok
		}

		// TODO: if `mc` is skip list with concurrent writes, could
		// perform better.
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

// full table scan.
func (snap *snapshot) iterator() api.Iterator {
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

	return reduceiter(scans)
}

// iterate on write store.
func (snap *snapshot) persistiterator() api.Iterator {
	if snap.mw != nil {
		return snap.mw.Scan()
	}
	return nil
}

// iterate on write store, read store, cache store and a latest disk store.
func (snap *snapshot) flushiterator(disk api.Index) api.Iterator {
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

	return reduceiter(scans)
}

func (snap *snapshot) windupiterator(disk api.Index) api.Iterator {
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

	return reduceiter(scans)
}

func (snap *snapshot) set(key, value, oldvalue []byte) ([]byte, uint64) {
	return snap.mw.Set(key, value, oldvalue)
}

func (snap *snapshot) setCAS(
	key, value, oldvalue []byte, cas uint64) ([]byte, uint64, error) {
	return snap.mw.SetCAS(key, value, oldvalue, cas)
}

func (snap *snapshot) delete(key, value []byte, lsm bool) ([]byte, uint64) {
	return snap.mw.Delete(key, value, lsm)
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

func (snap *snapshot) trypurge() {
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

func (snap *snapshot) attributes() string {
	flags := []string{}
	if snap.mw != nil {
		flags = append(flags, "w")
	}
	if snap.mr != nil {
		flags = append(flags, "r")
	}
	if snap.mc != nil {
		flags = append(flags, "c")
	}
	flags = append(flags, " ")

	ints := []string{}
	for i, disk := range snap.disks {
		if disk != nil {
			ints = append(ints, strconv.Itoa(i))
		}
	}
	memlevels := strings.Join(flags, "")
	disklevels := strings.Join(ints, ",")
	return "<" + memlevels + " " + disklevels + ">"
}

func compactiterator(disks []api.Index) api.Iterator {
	var ref [20]api.Iterator
	scans := ref[:0]

	for _, disk := range disks {
		if iter := disk.Scan(); iter != nil {
			scans = append(scans, iter)
		}
	}
	return reduceiter(scans)
}

func reduceiter(scans []api.Iterator) api.Iterator {
	if len(scans) == 0 {
		return nil
	}
	scan := scans[len(scans)-1]
	for i := len(scans) - 2; i >= 0; i-- {
		scan = lsm.YSort(scan, scans[i])
	}
	return scan
}
