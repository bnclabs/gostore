package bogn

import "fmt"
import "time"
import "unsafe"
import "sync/atomic"
import "runtime/debug"

import "github.com/prataprc/golog"
import "github.com/prataprc/gostore/api"
import "github.com/prataprc/gostore/lib"

// Compacttick timer tick to check for memory overflow, persisting
// flushing, compacting memory store and disk store.
var Compacttick = time.Duration(1 * time.Second)

func compactor(bogn *Bogn) {
	atomic.AddInt64(&bogn.nroutines, 1)
	log.Infof("%v starting compactor", bogn.logprefix)
	defer func() {
		if r := recover(); r != nil {
			log.Errorf("%v compactor crashed %v", bogn.logprefix, r)
			log.Errorf("\n%s", lib.GetStacktrace(2, debug.Stack()))
		} else {
			log.Infof("%v stopped compactor", bogn.logprefix)
		}

		atomic.AddInt64(&bogn.nroutines, -1)
		if snap := bogn.currsnapshot(); snap != nil {
			snap.release()
		}
	}()

	ticker := time.NewTicker(Compacttick)
	docompact := makecompactor(bogn)
	var respch chan api.Index
	var errch chan error

	// atmost two concurrent compaction can run.
	// a. compacting data in memory with latest level on disk (aka flush).
	// b. compacting data betwen two disk levels.

	// disk - latest level on disk
	// fdisk - memory data being flushed disk
	// disk0 - newer disk among compacted disks.
	// disk1 - older disk among compacted disks.
	// ndisk - compacted {level,version} of disk0, and disk1.
	var disk0, disk1 api.Index
	var nextlevel int

loop:
	for range ticker.C {
		if bogn.isclosed() {
			break loop
		} else if bogn.durable == false {
			continue
		}

		if respch == nil { // try to start disk compaction.
			disk0, disk1, nextlevel = bogn.pickcompactdisks()
			if nextlevel >= 0 {
				respch, errch = startdisk(bogn, disk0, disk1, nextlevel)
			}
		}

		if err := docompact(disk0); err != nil {
			panic(err)

		} else {
			select {
			case ndisk := <-respch:
				if err := findisk(bogn, disk0, disk1, ndisk); err != nil {
					panic(err)
				}
				respch, errch = nil, nil

			case err := <-errch:
				panic(err)

			default:
			}
		}
	}

	if err := dowindup(bogn); err != nil {
		panic(err)
	}
}

func makecompactor(bogn *Bogn) func(api.Index) error {
	mwcap := float64(bogn.memcapacity)
	// adaptive threshold.
	mwthreshold := int64(mwcap * .9) // start with 90% of configured capacity
	if bogn.workingset {             // start with 30% of configured capacity
		mwthreshold = int64(mwcap * .3)
	} else if bogn.dgm { // start with 50% of configured capacity
		mwthreshold = int64(mwcap * .5)
	}

	return func(disk0 api.Index) error {
		snap, overflow := bogn.currsnapshot(), false
		if snap != nil {
			overflow = snap.memheap() > mwthreshold
		}

		if atomic.LoadInt64(&bogn.dgmstate) == 0 { // fullset in memory
			if overflow { // fallback to dgm mode.
				atomic.StoreInt64(&bogn.dgmstate, 1)
				mwthreshold = int64(mwcap * .5) // start with 50% of capacity

			} else if bogn.flushelapsed() {
				return dopersist(bogn)

			} else {
				return nil // all is fine do nothing.
			}
		}

		if overflow {
			err := doflush(bogn, disk0)
			// adaptive threshold
			bgheap := float64(snap.memheap())
			mwthreshold += int64((mwcap - float64(mwthreshold)) - bgheap)
			return err
		}
		return nil
	}
}

// called only when full data set in memory.
// TODO: support `workingset` even when `dgm` is false.
func dopersist(bogn *Bogn) (err error) {
	snap := bogn.currsnapshot()

	lvl, level, version := -1, len(snap.disks)-1, 1
	if disk := snap.disks[level]; disk != nil {
		if lvl, version, _ = bogn.path2level(disk.ID()); lvl != level {
			panic(fmt.Errorf("mismatch in level %v != %v", level, lvl))
		}
	}

	now, iter := time.Now(), snap.persistiterator()
	ndisk, count, err := bogn.builddiskstore(level, version, iter)
	if err != nil {
		return err
	}
	fmsg := "%v took %v to build %v with %v entries\n"
	log.Infof(fmsg, bogn.logprefix, time.Since(now), ndisk.ID(), count)

	// organise the next set of stores, keep the write-store as it is
	// and update the latest disk store.
	var disks [16]api.Index
	disks[level] = ndisk

	func() {
		bogn.snaprw.Lock()
		defer bogn.snaprw.Unlock()

		head := newsnapshot(bogn, snap.mw, nil, nil, disks)
		head.yget = head.mw.Get
		atomic.StorePointer(&head.next, unsafe.Pointer(snap))
		head.refer()
		bogn.setheadsnapshot(head)
		snap.release()

		snap.addtopurge(snap.disklevels([]api.Index{})...)
		fmsg := "%v for snapshot %v compact persisted %v"
		log.Infof(fmsg, snap.bogn.logprefix, head.id, ndisk.ID())
	}()

	return nil
}

func doflush(bogn *Bogn, disk0 api.Index) (err error) {
	snap := bogn.currsnapshot()
	disk, nlevel, nversion := bogn.pickflushdisk(disk0)
	if nlevel < 0 {
		panic("impossible situation")
	}

	func() {
		bogn.snaprw.Lock()
		defer bogn.snaprw.Unlock()

		var mw api.Index
		if mw, err = bogn.newmemstore("mw", snap.mwseqno()); err != nil {
			panic(err) // should never happen
		}
		head := newsnapshot(bogn, mw, snap.mw, snap.mc, snap.disks)
		atomic.StorePointer(&head.next, unsafe.Pointer(snap))
		head.refer()
		bogn.setheadsnapshot(head)
		snap.release()

		fmsg := "%v for snapshot %v compact flush ..."
		log.Infof(fmsg, snap.bogn.logprefix, head.id)
	}()

	snap = bogn.latestsnapshot()
	now, iter := time.Now(), snap.flushiterator(disk)
	ndisk, count, err := bogn.builddiskstore(nlevel, nversion, iter)
	if err != nil {
		return err
	}
	fmsg := "%v took %v to build %v with %v entries\n"
	log.Infof(fmsg, bogn.logprefix, time.Since(now), ndisk.ID(), count)

	var disks [16]api.Index
	copy(disks[:], snap.disks[:])
	disks[nlevel] = ndisk
	if disk != nil {
		level, _, _ := bogn.path2level(disk.ID())
		disks[level] = nil
	}

	func() {
		bogn.snaprw.Lock()
		defer bogn.snaprw.Unlock()

		var mc api.Index

		if bogn.workingset {
			if mc, err = bogn.newmemstore("mc", 0); err != nil {
				panic(err) // should never happen
			}
		}

		// TODO: between head1 and head newly cached entries would be
		// lost.
		head := newsnapshot(bogn, snap.mw, nil, mc, disks)
		atomic.StorePointer(&head.next, unsafe.Pointer(snap))
		head.refer()
		bogn.setheadsnapshot(head)
		snap.release()

		snap.addtopurge(snap.mr, snap.mc, disk)
		fmsg := "%v for snapshot %v compact flush to %v"
		log.Infof(fmsg, snap.bogn.logprefix, head.id, ndisk.ID())
	}()

	return nil
}

func startdisk(
	bogn *Bogn,
	disk0, disk1 api.Index, nextlevel int) (chan api.Index, chan error) {

	var version int

	snap := bogn.currsnapshot()
	ndisk, version := snap.disks[nextlevel], 1
	if ndisk != nil {
		_, version, _ = bogn.path2level(ndisk.ID())
	}

	respch, errch := make(chan api.Index, 1), make(chan error, 1)
	iter := snap.compactiterator(disk0, disk1)

	go func() {
		fmsg := "%v for snapshot %v compacting %q + %q ..."
		log.Infof(fmsg, bogn.logprefix, snap.id, disk0.ID(), disk1.ID())

		now := time.Now()
		ndisk, count, err := bogn.builddiskstore(nextlevel, version+1, iter)
		if err != nil {
			errch <- err
			return
		}
		fmsg = "%v took %v to build %v with %v entries\n"
		log.Infof(fmsg, bogn.logprefix, time.Since(now), ndisk.ID(), count)

		respch <- ndisk
	}()
	return respch, errch
}

func findisk(bogn *Bogn, disk0, disk1, ndisk api.Index) error {
	snap := bogn.currsnapshot()

	var disks [16]api.Index
	copy(disks[:], snap.disks[:])
	level0, _, _ := bogn.path2level(disk0.ID())
	level1, _, _ := bogn.path2level(disk1.ID())
	nlevel, _, _ := bogn.path2level(ndisk.ID())
	disks[level0], disks[level1], disks[nlevel] = nil, nil, ndisk

	func() {
		bogn.snaprw.Lock()
		defer bogn.snaprw.Unlock()

		head := newsnapshot(bogn, snap.mw, snap.mr, snap.mc, disks)
		atomic.StorePointer(&head.next, unsafe.Pointer(snap))
		head.refer()
		bogn.setheadsnapshot(head)
		snap.release()

		snap.addtopurge(disk0, disk1)
		fmsg := "%v for snapshot %v compact disk"
		log.Infof(fmsg, snap.bogn.logprefix, head.id, ndisk.ID())
	}()
	return nil
}

func dowindup(bogn *Bogn) error {
	if bogn.durable == false {
		return nil
	}
	snap := bogn.currsnapshot()
	disk, nlevel, nversion := bogn.pickflushdisk(nil)
	if nlevel < 0 {
		panic("impossible situation")
	}
	now, iter := time.Now(), snap.windupiterator(disk)
	ndisk, count, err := bogn.builddiskstore(nlevel, nversion, iter)
	if err != nil {
		return err
	}
	fmsg := "%v took %v to build %v with %v entries\n"
	log.Infof(fmsg, bogn.logprefix, time.Since(now), ndisk.ID(), count)

	func() {
		bogn.snaprw.Lock()
		defer bogn.snaprw.Unlock()

		head := newsnapshot(bogn, nil, nil, nil, snap.disks)
		atomic.StorePointer(&head.next, unsafe.Pointer(snap))
		head.refer()
		bogn.setheadsnapshot(head)
		snap.release()

		snap.addtopurge(snap.mw, snap.mr, snap.mc, disk)
		fmsg := "%v for snapshot %v windup ..."
		log.Infof(fmsg, snap.bogn.logprefix, head.id, ndisk.ID())
	}()
	return nil
}
