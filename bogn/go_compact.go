package bogn

import "time"
import "unsafe"
import "sync/atomic"
import "runtime/debug"

import "github.com/bnclabs/gostore/api"
import "github.com/bnclabs/gostore/lib"
import s "github.com/bnclabs/gosettings"
import humanize "github.com/dustin/go-humanize"

// Compacttick timer tick to check for memory overflow, persistance,
// flushing, compacting memory stores and/or disk stores. Purger
// routine uses this tick to periodically purge un-referenced storage
// instance.
var Compacttick = time.Duration(1 * time.Second)

// list worker functions
// dopersist(bogn *Bogn) (err error)
// doflush(bogn *Bogn, disk0 api.Index) (err error)
//   startdisk( bogn *Bogn, disk0, disk1 api.Index, nlevel int)
//   findisk(bogn *Bogn, disk0, disk1, ndisk api.Index) error
// dowindup(bogn *Bogn) error

func compactor(bogn *Bogn) {
	infof("%v starting compactor ...", bogn.logprefix)

	defer func() {
		if r := recover(); r != nil {
			errorf("%v compactor crashed %v", bogn.logprefix, r)
			errorf("\n%s", lib.GetStacktrace(2, debug.Stack()))
		} else {
			infof("%v stopped compactor", bogn.logprefix)
		}

		atomic.AddInt64(&bogn.nroutines, -1)
		if snap := bogn.currsnapshot(); snap != nil {
			snap.release()
		}
	}()

	atomic.AddInt64(&bogn.nroutines, 1)
	ticker := time.NewTicker(Compacttick)
	doflushing := makeflusher(bogn)

	var respch chan api.Index
	var errch chan error

	// atmost two concurrent compaction can run.
	// a. compacting data in memory with latest level on disk, doflush().
	// b. compacting data betwen two disk levels, doflushing().

	// disk  - latest level on disk
	// disk0 - newer disk among compacted disks.
	// disk1 - older disk among compacted disks.
	// ndisk - compacted {level,version} of disk0, and disk1.
	var disk0, disk1 api.Index
	var nextlevel int

loop:
	for range ticker.C {
		if bogn.isclosed() && respch == nil {
			break loop

		} else if bogn.durable == false { // disk is not involved.
			continue loop
		}

		if respch == nil { // try to start disk compaction.
			disk0, disk1, nextlevel = bogn.pickcompactdisks()
			if nextlevel >= 0 {
				respch, errch = startdisk(bogn, disk0, disk1, nextlevel)
			}
		}

		if respch != nil { // if ongoing compaction, check for response
			select {
			case ndisk := <-respch:
				if err := findisk(bogn, disk0, disk1, ndisk); err != nil {
					panic(err)
				}
				disk0, disk1, respch, errch = nil, nil, nil, nil
				if bogn.isclosed() {
					break loop
				}

			case <-errch:
				disk0, disk1, respch, errch = nil, nil, nil, nil
				if bogn.isclosed() {
					break loop
				}

			default:
				if bogn.isclosed() {
					continue loop
				}
			}
		}

		if err := doflushing(disk0); err != nil {
			panic(err)
		}
	}

	if err := dowindup(bogn); err != nil {
		panic(err)
	}
}

func makeflusher(bogn *Bogn) func(api.Index) error {
	memcap := float64(bogn.memcapacity)
	// adaptive threshold.
	mwthreshold := int64(memcap * .9) // start with 90% of configured capacity
	if bogn.workingset {              // start with 30% of configured capacity
		mwthreshold = int64(memcap * .3)
	} else if bogn.dgm { // start with 50% of configured capacity
		mwthreshold = int64(memcap * .5)
	}

	stra := humanize.Bytes(uint64(bogn.memcapacity))
	strb := humanize.Bytes(uint64(mwthreshold))
	infof("%v memory threshold at %v of %v\n", bogn.logprefix, strb, stra)

	return func(disk0 api.Index) error {
		snap := bogn.currsnapshot()
		overflow := snap.memheap() > mwthreshold

		if !snap.isdirty() {
			debugf("%v no new mutations on snapshot %v", bogn.logprefix, snap.id)
			return nil
		}

		if atomic.LoadInt64(&bogn.dgmstate) == 0 { // fullset in memory
			if overflow { // fallback to dgm mode.
				atomic.StoreInt64(&bogn.dgmstate, 1)
				mwthreshold = int64(memcap * .5) // start with 50% of capacity

			} else if bogn.flushelapsed() {
				return dopersist(bogn)

			} else {
				return nil // all is fine don't touch the disk.
			}
		}

		if flushelapsed := bogn.flushelapsed(); overflow || flushelapsed {
			err := doflush(bogn, disk0, overflow, flushelapsed)

			// adaptive threshold
			mwthreshold = int64(memcap - float64(snap.memheap()))
			strb = humanize.Bytes(uint64(mwthreshold))
			fmsg := "%v new memory threshold at %v of %v\n"
			infof(fmsg, bogn.logprefix, strb, stra)
			return err
		}
		return nil
	}
}

// called only when full data set in memory.
// TODO: support `workingset` even when `dgm` is false.
func dopersist(bogn *Bogn) (err error) {
	infof("%v dopersist ...", bogn.logprefix)

	snap := bogn.currsnapshot()

	level := len(snap.disks) - 1
	nversion := bogn.nextdiskversion(level)
	disksetts := bogn.settingstodisk()

	// iterate on snap.mw
	iter, uuid := snap.persistiterator(), bogn.newuuid()
	ndisk, err := bogn.builddiskstore(
		level, nversion, uuid, "" /*flushunix*/, disksetts, iter,
	)
	if err != nil {
		return err
	}
	iter(true /*fin*/)

	// organise the next set of stores, keep the write-store as it is
	// and update the latest disk store.
	var disks [16]api.Index
	disks[level] = ndisk

	func() {
		bogn.snaplock()
		defer bogn.snapunlock()

		seqno := bogn.Getseqno()
		head := newsnapshot(bogn, snap.mw, nil, nil, disks, uuid, seqno)
		head.yget = head.mw.Get
		atomic.StorePointer(&head.next, unsafe.Pointer(snap))
		head.refer()
		bogn.setheadsnapshot(head)

		snap.addtopurge(snap.disklevels([]api.Index{})...)
		snap.release()

		fmsg := "%v new snapshot %v after persistance %v"
		infof(fmsg, snap.bogn.logprefix, head.attributes(), ndisk.ID())
	}()

	return nil
}

func doflush(bogn *Bogn, disk0 api.Index, overflow, elapsed bool) (err error) {
	cause := "overflow"
	if overflow && elapsed {
		panic("impossible situation")
	} else if (!overflow) && (!elapsed) {
		panic("impossible situation")
	} else if overflow == false && elapsed == true {
		cause = "elapsed"
	}
	infof("%v doflush (%v) ...", bogn.logprefix, cause)

	snap := bogn.currsnapshot()

	// move mw -> mr

	var mwseqno uint64

	uuid := bogn.newuuid()
	func() {
		bogn.snaplock()
		defer bogn.snapunlock()

		fmsg := "%v snapshot %v moved ahead from %v to %v (heap: %v)"
		from, bgheap := snap.beginseqno, humanize.Bytes(snap.memheap())
		mwseqno = snap.mwseqno()
		infof(fmsg, bogn.logprefix, snap.id, from, mwseqno, bgheap)

		var mw api.Index
		if mw, err = bogn.newmemstore("mw", mwseqno); err != nil {
			panic(err) // should never happen
		}
		head := newsnapshot(
			bogn, mw, snap.mw, snap.mc, snap.disks, uuid, mwseqno,
		)
		atomic.StorePointer(&head.next, unsafe.Pointer(snap))
		head.refer()
		bogn.setheadsnapshot(head)

		snap.release()
		snap = head

		fmsg = "%v new snapshot %v preparing for flush %v ..."
		infof(fmsg, head.bogn.logprefix, head.attributes(), head.id)
	}()
	// flush mr [+ mc] [+ disk] -> disk

	disk, nlevel := bogn.pickflushdisk(disk0)
	nversion := bogn.nextdiskversion(nlevel)
	disksetts := bogn.settingstodisk()

	// iterate on snap.mr [+ snap.mc] [+ disk]
	uuid = bogn.newuuid()
	iter := snap.flushiterator(disk)
	ndisk, err := bogn.builddiskstore(
		nlevel, nversion, uuid, "" /*flushunix*/, disksetts, iter,
	)
	if err != nil {
		return err
	}
	iter(true /*fin*/)

	var disks [16]api.Index
	copy(disks[:], snap.disks[:])
	if disk != nil {
		level, _, _ := bogn.path2level(disk.ID())
		disks[level] = nil
	}
	disks[nlevel] = ndisk

	func() {
		bogn.snaplock()
		defer bogn.snapunlock()

		var mc api.Index

		if bogn.workingset {
			if mc, err = bogn.newmemstore("mc", 0); err != nil {
				panic(err) // should never happen
			}
		}

		// TODO: between head1 and head newly cached entries would be
		// lost.
		head := newsnapshot(bogn, snap.mw, nil, mc, disks, uuid, mwseqno)
		atomic.StorePointer(&head.next, unsafe.Pointer(snap))
		head.refer()
		bogn.setheadsnapshot(head)

		snap.addtopurge(snap.mr, snap.mc, disk)
		snap.release()

		fmsg := "%v new snapshot %v after flush to %v"
		infof(fmsg, snap.bogn.logprefix, head.attributes(), ndisk.ID())
	}()

	return nil
}

func startdisk(
	bogn *Bogn,
	disk0, disk1 api.Index, nlevel int) (chan api.Index, chan error) {

	infof("%v startdisk ...", bogn.logprefix)

	respch, errch := make(chan api.Index, 1), make(chan error, 1)
	iter, uuid := compactiterator(disk0, disk1), bogn.newuuid()
	nversion := bogn.nextdiskversion(nlevel)
	disksetts := (s.Settings{}).Mixin(bogn.settingsfromdisk(disk0))
	flushunix := bogn.getflushunix(disk0)

	go func() {
		fmsg := "%v start disk compaction %q + %q ..."
		infof(fmsg, bogn.logprefix, disk0.ID(), disk1.ID())

		ndisk, err := bogn.builddiskstore(
			nlevel, nversion, uuid, flushunix, disksetts, iter,
		)
		if err != nil {
			errch <- err
			return
		}
		iter(true /*fin*/)

		respch <- ndisk
	}()
	return respch, errch
}

func findisk(bogn *Bogn, disk0, disk1, ndisk api.Index) error {
	infof("%v findisk ...", bogn.logprefix)

	snap := bogn.currsnapshot()

	var disks [16]api.Index
	copy(disks[:], snap.disks[:])
	level0, _, _ := bogn.path2level(disk0.ID())
	level1, _, _ := bogn.path2level(disk1.ID())
	nlevel, _, _ := bogn.path2level(ndisk.ID())
	disks[level0], disks[level1], disks[nlevel] = nil, nil, ndisk

	_, _, uuid := bogn.path2level(ndisk.ID())
	func() {
		bogn.snaplock()
		defer bogn.snapunlock()

		seqno := bogn.Getseqno()
		head := newsnapshot(bogn, snap.mw, snap.mr, snap.mc, disks, uuid, seqno)
		atomic.StorePointer(&head.next, unsafe.Pointer(snap))
		head.refer()
		bogn.setheadsnapshot(head)

		snap.addtopurge(disk0, disk1)
		snap.release()

		fmsg := "%v new snapshot %v after to compact disk %v"
		infof(fmsg, snap.bogn.logprefix, head.attributes(), ndisk.ID())
	}()
	return nil
}

func dowindup(bogn *Bogn) error {
	infof("%v dowindup ...", bogn.logprefix)

	if bogn.durable == false {
		return nil
	}

	snap := bogn.currsnapshot()
	if !snap.isdirty() {
		infof("%v no new mutations on snapshot %v", bogn.logprefix, snap.id)
		return nil
	}
	fmsg := "%v snapshot %v moved ahead from %v to %v (heap: %v)"
	mwseqno, bgheap := snap.mwseqno(), humanize.Bytes(snap.memheap())
	infof(fmsg, bogn.logprefix, snap.id, snap.beginseqno, mwseqno, bgheap)

	var purgedisk api.Index
	var nlevel int
	if bogn.dgmstate == 0 { // full set in memory
		purgedisk, nlevel = snap.disks[len(snap.disks)-1], len(snap.disks)-1
	} else {
		purgedisk, nlevel = bogn.pickflushdisk(nil)
	}
	nversion := bogn.nextdiskversion(nlevel)
	disksetts := bogn.settingstodisk()

	iter, uuid := snap.windupiterator(purgedisk), bogn.newuuid()
	ndisk, err := bogn.builddiskstore(
		nlevel, nversion, uuid, "" /*flushunix*/, disksetts, iter,
	)
	if err != nil {
		return err
	}
	iter(true /*fin*/)

	var disks [16]api.Index
	copy(disks[:], snap.disks[:])
	disks[nlevel] = ndisk

	func() {
		bogn.snaplock()
		defer bogn.snapunlock()

		seqno := bogn.Getseqno()
		head := newsnapshot(bogn, nil, nil, nil, disks, uuid, seqno)
		atomic.StorePointer(&head.next, unsafe.Pointer(snap))
		head.refer()
		bogn.setheadsnapshot(head)

		snap.addtopurge(snap.mw, snap.mr, snap.mc, purgedisk)
		snap.release()

		fmsg := "%v new snapshot %s windup on disk %v"
		infof(fmsg, snap.bogn.logprefix, head.attributes(), ndisk.ID())
	}()
	return nil
}
