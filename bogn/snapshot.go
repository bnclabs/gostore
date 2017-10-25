package bogn

import "fmt"

import "github.com/prataprc/gostore/api"
import "github.com/prataprc/gostore/lib"
import "github.com/prataprc/gostore/lsm"
import "github.com/prataprc/gostore/llrb"

type snapshot struct {
	bogn       *Bogn
	mw, mr, mc api.Index
	disks      [16]api.Index
	yget       api.Getter

	setch   chan *setcache
	cachech chan *setcache
}

func opensnapshot(bogn *Bogn, disks [16]api.Index) (*snapshot, error) {
	var err error
	head := &snapshot{bogn: bogn, disks: disks}
	head.mw, err = bogn.newmemstore("mw", nil)
	if err != nil {
		return nil, err
	}
	if bogn.workingset {
		head.setch = make(chan *setcache, 1000)   // TODO: no magic number
		head.cachech = make(chan *setcache, 1000) // TODO: no magic number
		head.mc, err = bogn.newmemstore("mc", nil)
		if err != nil {
			return nil, err
		}
		go cacher(bogn, head.mc, head.setch, head.cachech)
	}
	head.yget = head.reduceyget()
	return head, nil
}

func newsnapshot(
	bogn *Bogn, mw, mr, mc api.Index, disks [16]api.Index) *snapshot {

	head := &snapshot{bogn: bogn, mw: mw, mr: mr, mc: mc}
	copy(head.disks[:], disks[:])
	if head.mc != nil {
		head.setch = make(chan *setcache, 1000)   // TODO: no magic number
		head.cachech = make(chan *setcache, 1000) // TODO: no magic number
		go cacher(bogn, head.mc, head.setch, head.cachech)
	}
	head.yget = head.reduceyget()
	return head
}

func (snap *snapshot) latestlevel() (int, api.Index) {
	for level, disk := range snap.disks {
		if disk != nil {
			if lvl, _ := snap.bogn.path2level(disk.ID()); level != lvl {
				panic(fmt.Errorf("mismatch in level %v != %v", level, lvl))
			}
			return level, disk
		}
	}
	return -1, nil
}

func (snap *snapshot) oldestlevel() (int, api.Index) {
	for level := len(snap.disks) - 1; level >= 0; level-- {
		if disk := snap.disks[level]; disk != nil {
			if lvl, _ := snap.bogn.path2level(disk.ID()); level != lvl {
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
			stats := v.Stats()
			heap += stats["node.heap"].(int64) + stats["value.heap"].(int64)
		case *llrb.MVCC:
			stats := v.Stats()
			heap += stats["node.heap"].(int64) + stats["value.heap"].(int64)
		}
	}
	return heap
}

func (snap *snapshot) reduceyget() (get api.Getter) {
	gets := []api.Getter{}
	gets = append(gets, snap.mw.Get)
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

	get = gets[len(gets)-1]
	for i := len(gets) - 2; i >= 0; i-- {
		get = lsm.YGet(get, gets[i])
	}
	return
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

	scans = append(scans, snap.mw.Scan())
	if snap.mr != nil {
		scans = append(scans, snap.mr.Scan())
	}
	for _, disk := range snap.disklevels([]api.Index{}) {
		scans = append(scans, disk.Scan())
	}

	scan = scans[len(scans)-1]
	for i := len(scans) - 2; i >= 0; i-- {
		scan = lsm.YSort(scan, scans[i])
	}
	return
}

func (snap *snapshot) persistiterator() (scan api.Iterator) {
	var ref [20]api.Iterator
	scans := ref[:0]

	scans = append(scans, snap.mw.Scan())
	for _, disk := range snap.disklevels([]api.Index{}) {
		scans = append(scans, disk.Scan())
	}

	scan = scans[len(scans)-1]
	for i := len(scans) - 2; i >= 0; i-- {
		scan = lsm.YSort(scan, scans[i])
	}
	return
}

func (snap *snapshot) flushiterator(disk api.Index) (scan api.Iterator) {
	var ref [20]api.Iterator
	scans := ref[:0]

	scans = append(scans, snap.mw.Scan())
	if snap.mr != nil {
		scans = append(scans, snap.mr.Scan())
	}
	if snap.mc != nil {
		scans = append(scans, snap.mc.Scan())
	}
	if disk != nil {
		scans = append(scans, disk.Scan())
	}

	scan = scans[len(scans)-1]
	for i := len(scans) - 2; i >= 0; i-- {
		scan = lsm.YSort(scan, scans[i])
	}
	return
}

func (snap *snapshot) compactiterator(d0, d1 api.Index) (scan api.Iterator) {
	if d0 == nil {
		return d1.Scan()
	} else if d1 == nil {
		return d0.Scan()
	}
	scan = lsm.YSort(d1.Scan(), d0.Scan())
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
}
