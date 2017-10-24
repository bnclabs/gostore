package bogr

import "github.com/prataprc/gostore/api"
import "github.com/prataprc/gostore/lib"
import "github.com/prataprc/gostore/llrb"
import "github.com/prataprc/gostore/bubt"

type bogrsnapshot struct {
	bogr       *Bogn
	next       *bogrsnapshot
	mw, mr, mc *llrb.LLRB
	disks      []*bubt.Snapshot
	yget       api.Getter

	setch   chan *setcache
	cachech chan *setcache
}

func newsnapshot(
	bogr *Bogn, mw, mr, mc *llrb.LLRB,
	disks []*bubt.Snapshot, next *bogrsnapshot) *bogrsnapshot {

	head := bogrsnapshot{
		bogr: bogr, mw: mw, mr: mr, mc: mc, disks: disks, next: next,
	}
	if head.mc != nil {
		head.setch = make(chan *setcache, 1000)   // TODO: no magic number
		head.cachech = make(chan *setcache, 1000) // TODO: no magic number
	}
	return head
}

func (bs *bogrsnapshot) reduceyget() (get api.Getter) {
	gets := []api.Getter{}
	gets = append(gets, bs.mw.Get)
	if bs.mr != nil {
		gets = append(gets, bs.mr.Get)
	}
	if bs.mc != nil {
		gets = append(gets, bs.mc.Get)
	}

	var dget api.Getter
	for i, disk := range bs.disks {
		if disk != nil {
			if dget != nil && bs.mc != nil {
				dget = bs.cachingget(disk.get)
			} else {
				dget = disk.Get
			}
			gets = append(gets, dget)
		}
	}

	if len(gets) > 1 {
		get = gets[len(gets)-1].Get
		for i := len(gets) - 2; i >= 0; i-- {
			get = lsm.YGet(get, gets[i])
		}
	} else {
		get = gets[0].Get
	}
	return
}

func (bs *bogrsnapshot) cachingget(get api.Getter) api.Getter {
	return func(key, value []byte) {
		value, cas, deleted, ok := get(key, value)
		if ok == false {
			return value, cas, deleted, ok
		}

		// TODO: if `mc` as skip list with concurrent writes could
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

func (bs *bogrsnapshot) reduceyscan() (scan api.Iterator) {
	var ref [20]api.Iterator
	scans := ref[:0]

	scans = append(scans, bs.mw.Scan)
	if bs.mr != nil {
		scans = append(scans, bs.mr.Scan)
	}
	for _, disk := range bs.disks {
		scans = append(scans, disk.Scan)
	}

	if len(scans) > 1 {
		scan = scans[len(scans)-1].Scan
		for i := len(scans) - 2; i >= 0; i-- {
			scan = lsm.YSort(scan, scans[i])
		}
	} else {
		scan = scans[0].Scan
	}
	return
}

func (bs *bogrsnapshot) destroy() {
	close(bs.setch)
	// `mc` will be destroyed by the cacher.
}

type setcache struct {
	key     []byte
	value   []byte
	seqno   []byte
	deleted bool
}

func (bs *bogrsnapshot) cacher(
	setch, cachech chan *setcache, fin chan struct{}) {

	for cmd := range setch {
		bs.mc.Setseqno(cmd.seqno - 1)
		if cmd.deleted {
			// delete in lsm mode.
			if _, cas := bs.mc.Delete(key, nil, true); cas != cmd.seqno {
				panic("impossible situation")
			}
		} else if _, cas := bs.mc.Set(key, value, nil); cas != cmd.seqno {
			panic("impossible situation")
		}
		cachech <- cmd
	}
}
