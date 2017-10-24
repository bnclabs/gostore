package bogn

import "github.com/prataprc/gostore/api"
import "github.com/prataprc/gostore/lib"

type snapshot struct {
	bogn       *Bogn
	mw, mr, mc api.Index
	disks      [16]api.Index
	yget       api.Getter

	setch   chan *setcache
	cachech chan *setcache
}

func newsnapshot(
	bogn *Bogn, mw, mr, mc api.Index, disks [16]api.Index) *snapshot {

	head := snapshot{bogn: bogn, mw: mw, mr: mr, mc: mc}
	copy(head.disks, disks)
	if head.mc != nil {
		head.setch = make(chan *setcache, 1000)   // TODO: no magic number
		head.cachech = make(chan *setcache, 1000) // TODO: no magic number
		go cacher(bogn, head.mc, head.setch, head.cachech)
	}
	head.yget = head.reduceyget()
	return head
}

func (bs *snapshot) getseqno() uint64 {
	bs.bogn.rw.Lock()
	seqno := bs.mw.Getseqno()
	bs.bogn.rw.Unlock()
	return seqno
}

func (bs *snapshot) latestlevel() (int, api.Index) {
	for level, disk := range bs.disks {
		if disk != nil {
			return level, disk
		}
	}
	return -1, nil
}

func (bs *snapshot) oldestlevel() (int, api.Index) {
	for level := len(bs.disks) - 1; level >= 0; level-- {
		if disk := bs.disks[i]; disk != nil {
			return level, disk
		}
	}
	return -1, nil
}

func (bs *snapshot) disklevels(disks []api.Index) []api.Index {
	for _, disk := range bs.disks {
		if disk != nil {
			disks = append(disks, disk)
		}
	}
	return disks
}

func (bs *snapshot) memheap() int64 {
	memindexes, heap := []api.Index{bs.mw, ms.mr, ms.mc}, int64(0)
	for _, memindex := range memindexes {
		switch index := memindex.(type) {
		case *llrb.LLRB:
			stats := index.Stats()
			heap += stats["node.heap"].(int64) + stats["value.heap"].(int64)
		case *llrb.MVCC:
			stats := index.Stats()
			heap += stats["node.heap"].(int64) + stats["value.heap"].(int64)
		}
	}
	return heap
}

func (bs *snapshot) reduceyget() (get api.Getter) {
	gets := []api.Getter{}
	gets = append(gets, bs.mw.Get)
	if bs.mr != nil {
		gets = append(gets, bs.mr.Get)
	}
	if bs.mc != nil {
		gets = append(gets, bs.mc.Get)
	}

	var dget api.Getter
	for i, disk := range bs.disklevels() {
		if dget != nil && bs.mc != nil {
			dget = bs.cachingget(disk.get)
		} else {
			dget = disk.Get
		}
		gets = append(gets, dget)
	}

	get = gets[len(gets)-1].Get
	for i := len(gets) - 2; i >= 0; i-- {
		get = lsm.YGet(get, gets[i])
	}
	return
}

func (bs *snapshot) cachingget(get api.Getter) api.Getter {
	return func(key, value []byte) {
		value, cas, deleted, ok := get(key, value)
		if ok == false {
			return value, cas, deleted, ok
		}

		// TODO: if `mc` is skip list with concurrent writes, could
		// be an optimized solution.
		select {
		case cmd := <-cachech:
			cmd.key = lib.Fixbuffer(cmd.key, int64(len(key)))
			copy(cmd.key, key)
			cmd.value = lib.Fixbuffer(cmd.value, int64(len(value)))
			copy(cmd.value, value)
			cmd.seqno = cas
			cmd.deleted = deleted
			select {
			case bs.setch <- cmd:
			default:
			}

		default:
		}
		return value, cas, deleted, ok
	}
}

func (bs *snapshot) iterator() (scan api.Iterator) {
	var ref [20]api.Iterator
	scans := ref[:0]

	scans = append(scans, bs.mw.Scan)
	if bs.mr != nil {
		scans = append(scans, bs.mr.Scan)
	}
	for _, disk := range bs.disklevels() {
		scans = append(scans, disk.Scan)
	}

	scan = scans[len(scans)-1].Scan
	for i := len(scans) - 2; i >= 0; i-- {
		scan = lsm.YSort(scan, scans[i])
	}
	return
}

func (bs *snapshot) flushiterator(disk api.Index) (scan api.Iterator) {
	var ref [20]api.Iterator
	scans := ref[:0]

	scans = append(scans, bs.mw.Scan)
	if bs.mr != nil {
		scans = append(scans, bs.mr.Scan)
	}
	if bs.mc != nil {
		scans = append(scans, bs.mc.Scan)
	}
	if disk != nil {
		scans = append(disk.Scan)
	}

	scan = scans[len(scans)-1].Scan
	for i := len(scans) - 2; i >= 0; i-- {
		scan = lsm.YSort(scan, scans[i])
	}
	return
}

func (bs *snapshot) compaciterator(d0, d1 api.Index) (scan api.Iterator) {
	if d0 == nil {
		return d1.Scan
	} else if d1 == nil {
		return d0.Scan
	}
	scan = lsm.YSort(d1.scan, d0.scans[i])
	return
}

func (bs *snapshot) delete(key, value []byte, lsm bool) ([]byte, uint64) {
	return bs.mw.Delete(key, value, lsm)
}

func (bs *snapshot) set(key, value, oldvalue []byte) ([]byte, uint64) {
	return bs.mw.Set(key, value, oldvalue)
}

func (bs *snapshot) setCAS(
	key, value, oldvalue, cas uint64) ([]byte, uint64, error) {
	return bs.mw.SetCAS(key, value, oldvalue)
}

func (bs *snapshot) finish() {
	if bs != nil {
		close(bs.setch)
	}
}
