package bogn

import "fmt"
import "time"
import "unsafe"
import "strings"
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
// doflush(bogn *Bogn, disks []api.Index) (err error)
//   startdisk( bogn *Bogn, disks []api.Index, nlevel int)
//   findisk(bogn *Bogn, disks []api.Index, ndisk api.Index) error
// dowindup(bogn *Bogn) error

func posttick(bogn *Bogn) {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{"compact.autocommit", respch}
	lib.FailsafeRequest(bogn.compactorch, respch, cmd, bogn.finch)
}

func postfindisk(bogn *Bogn, ndisk api.Index, err error) {
	cmd := []interface{}{"compact.findisk", ndisk, err}
	lib.FailsafeRequest(bogn.compactorch, nil, cmd, nil)
}

func postcommit(bogn *Bogn, appdata []byte) {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{"compact.commit", appdata, respch}
	lib.FailsafeRequest(bogn.compactorch, respch, cmd, nil)
}

func compactorclose(bogn *Bogn) {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{"compact.close", respch}
	lib.FailsafeRequest(bogn.compactorch, respch, cmd, nil)
}

func tombstonepurge(bogn *Bogn) {
	respch := make(chan []interface{}, 1)
	cmd := []interface{}{"compact.tombstonepurge", respch}
	lib.FailsafeRequest(bogn.compactorch, respch, cmd, nil)
}

func compactor(bogn *Bogn, compactorch chan []interface{}) {
	infof("%v rcompactor: starting ...", bogn.logprefix)

	atomic.AddInt64(&bogn.nroutines, 1)
	defer func() {
		if r := recover(); r != nil {
			errorf("%v compactor crashed %v", bogn.logprefix, r)
			errorf("\n%s", lib.GetStacktrace(2, debug.Stack()))
		} else {
			infof("%v rcompactor: stopped", bogn.logprefix)
		}

		atomic.AddInt64(&bogn.nroutines, -1)
		if snap := bogn.currsnapshot(); snap != nil {
			snap.release()
		}
	}()

	if bogn.autocommit > 0 {
		go compactticker(bogn, compactorch)
	}

	doflushing := makeflusher(bogn)

	// atmost two concurrent compaction can run.
	// a. compacting data in memory with latest level on disk, doflush().
	// b. compacting data betwen two disk levels, doflushing().

	// disk  - latest level on disk
	// disks - list of disks to compact
	// ndisk - compacted {level,version} of `disks`
	var disks []api.Index
	var what string
	var tspch chan []interface{}

	tombstonepurge, activecompaction, closed := false, false, false

	trystartdisk := func() {
		var nextlevel int
		disks, nextlevel, what = bogn.pickcompactdisks(tombstonepurge)
		if nextlevel >= 0 {
			startdisk(bogn, disks, nextlevel, what)
			activecompaction = true
		} else {
			disks, what, tspch = nil, "", nil
			activecompaction, tombstonepurge = false, false
		}
	}

	tryfindisk := func(ndisk api.Index, err error) {
		if err != nil {
			panic(err)

		} else if ndisk == nil {
			panic("impossible case")
		}

		if err := findisk(bogn, disks, ndisk); err != nil {
			panic(err)
		}
		if tombstonepurge && tspch != nil {
			tspch <- []interface{}{nil}
		}
		disks, what, tspch = nil, "", nil
		activecompaction, tombstonepurge = false, false
	}

	docmd := func(cmd []interface{}) {
		switch cmdname := cmd[0].(string); cmdname {
		case "compact.tombstonepurge":
			tombstonepurge, tspch = true, cmd[1].(chan []interface{})

		case "compact.autocommit":
			appdata, respch := []byte(nil), cmd[1].(chan []interface{})
			if bogn.durable { // disk is not involved.
				if activecompaction == false {
					trystartdisk()
				}
				// only blocking call !!
				if err := doflushing(disks, appdata); err != nil {
					panic(err)
				}
			}
			respch <- []interface{}{nil}

		case "compact.commit":
			appdata, respch := cmd[1].([]byte), cmd[2].(chan []interface{})
			if bogn.durable { // disk is not involved.
				if activecompaction == false {
					trystartdisk()
				}
				// only blocking call !!
				if err := doflushing(disks, appdata); err != nil {
					panic(err)
				}
			}
			respch <- []interface{}{nil}

		case "compact.findisk":
			a, b, ndisk, err := cmd[1], cmd[2], api.Index(nil), error(nil)
			if a != nil {
				ndisk = cmd[1].(api.Index)
			}
			if b != nil {
				err = cmd[2].(error)
			}
			tryfindisk(ndisk, err)

		case "compact.close":
			closed = true
			respch := cmd[1].(chan []interface{})
			if err := dowindup(bogn); err != nil {
				panic(err)
			}
			respch <- []interface{}{nil}
		}
	}

loop:
	for cmd := range compactorch {
		if closed == false || activecompaction {
			docmd(cmd)
		}
		if closed && activecompaction == false {
			break loop
		}
	}
}

func makeflusher(bogn *Bogn) func([]api.Index, []byte) error {
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
	fmsg := "%v compactor: start memory threshold at %v of %v\n"
	infof(fmsg, bogn.logprefix, strb, stra)

	return func(disks []api.Index, appdata []byte) error {
		snap := bogn.currsnapshot()
		overflow := snap.memheap() > mwthreshold

		if !snap.isdirty() {
			fmsg := "%v compactor: no new mutations on snapshot %v"
			debugf(fmsg, bogn.logprefix, snap.id)
			return nil
		}

		if atomic.LoadInt64(&bogn.dgmstate) == 0 { // fullset in memory
			if overflow { // fallback to dgm mode.
				atomic.StoreInt64(&bogn.dgmstate, 1)
				mwthreshold = int64(memcap * .5) // start with 50% of capacity

			} else if bogn.flushelapsed() {
				return dopersist(bogn, appdata)

			} else {
				return nil // all is fine don't touch the disk.
			}
		}

		if flushelapsed := bogn.flushelapsed(); overflow || flushelapsed {
			// adaptive threshold
			mwthreshold = int64(memcap - float64(snap.memheap()))
			strb = humanize.Bytes(uint64(mwthreshold))
			fmsg := "%v compactor: new memory threshold at %v of %v\n"
			infof(fmsg, bogn.logprefix, strb, stra)

			err := doflush(bogn, disks, overflow, flushelapsed, appdata)
			return err
		}
		return nil
	}
}

// called only when full data set in memory.
// TODO: support `workingset` even when `dgm` is false.
func dopersist(bogn *Bogn, appdata []byte) (err error) {
	infof("%v dopersist ...", bogn.logprefix)

	snap := bogn.currsnapshot()
	if !snap.isdirty() {
		fmsg := "%v dopersist: no new mutations on snapshot %v"
		infof(fmsg, bogn.logprefix, snap.id)
	}

	level := len(snap.disks) - 1
	nversion := bogn.nextdiskversion(level)
	disksetts := bogn.settingstodisk()

	// iterate on snap.mw
	itere, uuid := snap.persistiterator(), bogn.newuuid()
	ndisk, err := bogn.builddiskstore(
		"dopersist", level, nversion, uuid, "" /*flushunix*/, disksetts, itere,
		"" /*appendid*/, nil /*valuelogs*/, "persist", appdata,
	)
	if err != nil {
		return err
	}
	itere(true /*fin*/)
	bogn.addamplification(ndisk)
	lastseqno := bogn.getdiskseqno(ndisk)

	fmsg := "%v doperist: snapshot %v moved ahead from %v to %v"
	infof(fmsg, bogn.logprefix, snap.id, snap.beginseqno, lastseqno)

	// organise the next set of stores, keep the write-store as it is
	// and update the latest disk store.
	var disks [16]api.Index
	disks[level] = ndisk

	func() {
		bogn.snaplock()
		defer bogn.snapunlock()

		head := newsnapshot(bogn, snap.mw, nil, nil, disks, uuid, lastseqno)
		head.yget = head.mw.Get
		atomic.StorePointer(&head.next, unsafe.Pointer(snap))
		head.refer()
		bogn.setheadsnapshot(head)

		snap.addtopurge(snap.disklevels([]api.Index{})...)
		snap.release()

		fmsg = "%v dopersist: new snapshot %v after persistance %v"
		infof(fmsg, snap.bogn.logprefix, head.attributes(), ndisk.ID())
	}()

	return nil
}

func doflush(
	bogn *Bogn, disks []api.Index, overf, elapsed bool,
	appdata []byte) (err error) {

	cause := "overflow"
	if overf && elapsed {
		panic(fmt.Errorf("impossible situation"))
	} else if (!overf) && (!elapsed) {
		panic(fmt.Errorf("impossible situation"))
	} else if overf == false && elapsed == true {
		cause = "elapsed"
	}

	fdisks, nlevel, what := bogn.pickflushdisk(disks)
	if nlevel < 0 {
		infof("%v doflush: skipping %q", bogn.logprefix, what)
		return nil
	}

	ids := []string{"mw", "mc"}
	for _, d := range fdisks {
		ids = append(ids, d.ID())
	}
	fmsg := "%v doflush: (%v) as %q for %v"
	infof(fmsg, bogn.logprefix, cause, what, strings.Join(ids, " + "))

	var from, mwseqno uint64

	snap := bogn.currsnapshot()
	uuid := bogn.newuuid()
	func() {
		bogn.snaplock()
		defer bogn.snapunlock()

		fmsg := "%v doflush: snapshot %v moved ahead from %v to %v (heap: %v)"
		from, mwseqno = snap.beginseqno, snap.mwseqno()
		bgheap := humanize.Bytes(uint64(snap.memheap()))
		infof(fmsg, bogn.logprefix, snap.id, from, mwseqno, bgheap)

		var mw api.Index
		if mw, err = bogn.newmemstore("doflush", "mw", mwseqno); err != nil {
			panic(err) // should never happen
		}
		// it is expected that all mutations uptil mwseqno, the last
		// mutation on `snap`, will be flushed to disk.
		head := newsnapshot(
			bogn, mw, snap.mw, snap.mc, snap.disks, uuid, mwseqno,
		)
		atomic.StorePointer(&head.next, unsafe.Pointer(snap))
		head.refer()
		bogn.setheadsnapshot(head)

		snap.release()
		snap = head

		fmsg = "%v doflush: new snapshot %v preparing for flush %v ..."
		infof(fmsg, head.bogn.logprefix, head.attributes(), head.id)
	}()

	nversion := bogn.nextdiskversion(nlevel)
	disksetts := bogn.settingstodisk()

	// Finalize mr level, wait for read snapshot to catch up with tip.
	// it is safe to call finalize on `mr` because write operations are
	// not going to be on the new created `mw`.
	snap.finalizeindex(snap.mr)

	// iterate on snap.mr [+ snap.mc] [+ fdisks]
	uuid = bogn.newuuid()
	itere := snap.flushiterator(fdisks)
	appendid, valuelogs := bogn.indexvaluelogs(fdisks)
	ndisk, err := bogn.builddiskstore(
		"doflush", nlevel, nversion, uuid, "" /*flushunix*/, disksetts, itere,
		appendid, valuelogs, what, appdata,
	)
	if err != nil {
		return err
	}
	itere(true /*fin*/)

	bogn.addamplification(ndisk)

	if lastseqno := bogn.getdiskseqno(ndisk); lastseqno != mwseqno {
		panic(fmt.Errorf("lastseqno(%v) != mwseqno(%v)", lastseqno, mwseqno))
	}

	var ndisks [16]api.Index
	copy(ndisks[:], snap.disks[:])
	for _, disk := range fdisks {
		level, _, _ := bogn.path2level(disk.ID())
		ndisks[level] = nil
	}
	ndisks[nlevel] = ndisk

	func() {
		bogn.snaplock()
		defer bogn.snapunlock()

		var mc api.Index

		if bogn.workingset {
			if mc, err = bogn.newmemstore("doflush", "mc", 0); err != nil {
				panic(err) // should never happen
			}
		}

		// TODO: between head1 and head newly cached entries would be
		// lost.
		seqno := snap.beginseqno
		head := newsnapshot(bogn, snap.mw, nil, mc, ndisks, uuid, seqno)
		atomic.StorePointer(&head.next, unsafe.Pointer(snap))
		head.refer()
		bogn.setheadsnapshot(head)

		snap.addtopurge(snap.mr, snap.mc)
		snap.addtopurge(fdisks...)
		snap.release()

		fmsg := "%v doflush: new snapshot %v after flush to %v"
		infof(fmsg, snap.bogn.logprefix, head.attributes(), ndisk.ID())
	}()

	return nil
}

func startdisk(bogn *Bogn, disks []api.Index, nlevel int, what string) {
	infof("%v startdisk ...", bogn.logprefix)

	disk0 := disks[0]
	itere, uuid := compactiterator(disks), bogn.newuuid()
	nversion := bogn.nextdiskversion(nlevel)
	disksetts := (s.Settings{}).Mixin(bogn.settingsfromdisk(disk0))
	flushunix := bogn.getflushunix(disk0)
	appdata := bogn.getappdata(disk0)

	ids := []string{}
	for _, disk := range disks {
		ids = append(ids, disk.ID())
	}
	appendid, valuelogs := bogn.indexvaluelogs(disks)

	go func() {
		fmsg := "%v startdisk: compaction (%v) %v ..."
		infof(fmsg, bogn.logprefix, what, strings.Join(ids, " + "))

		ndisk, err := bogn.builddiskstore(
			"startdisk", nlevel, nversion, uuid, flushunix, disksetts, itere,
			appendid, valuelogs, what, appdata,
		)
		itere(true /*fin*/)
		if err != nil {
			postfindisk(bogn, nil, err)

		} else {
			bogn.addamplification(ndisk)
			postfindisk(bogn, ndisk, nil)
		}
	}()

	return
}

func findisk(bogn *Bogn, disks []api.Index, ndisk api.Index) error {
	infof("%v findisk ...", bogn.logprefix)

	func() {
		bogn.snaplock()
		defer bogn.snapunlock()

		var fdisks [16]api.Index
		snap := bogn.currsnapshot()

		copy(fdisks[:], snap.disks[:])
		for _, disk := range disks {
			level, _, _ := bogn.path2level(disk.ID())
			fdisks[level] = nil
		}
		nlevel, _, _ := bogn.path2level(ndisk.ID())
		fdisks[nlevel] = ndisk
		_, _, uuid := bogn.path2level(ndisk.ID())

		seqno, mw, mr, mc := snap.beginseqno, snap.mw, snap.mr, snap.mc
		head := newsnapshot(bogn, mw, mr, mc, fdisks, uuid, seqno)
		atomic.StorePointer(&head.next, unsafe.Pointer(snap))
		head.refer()
		bogn.setheadsnapshot(head)

		snap.addtopurge(disks[:]...)
		snap.release()

		fmsg := "%v findisk: new snapshot %v after to compact disk %v"
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
	if bogn.autocommit == 0 && snap.isdirty() {
		panic("when autocommit disabled cannot windup a dirty snapshot")
	}
	if !snap.isdirty() {
		fmsg := "%v dowindup: no new mutations on snapshot %v"
		infof(fmsg, bogn.logprefix, snap.id)
		return nil
	}
	fmsg := "%v dowindup: snapshot %v moved ahead from %v to %v (heap: %v)"
	mwseqno, bgheap := snap.mwseqno(), humanize.Bytes(uint64(snap.memheap()))
	infof(fmsg, bogn.logprefix, snap.id, snap.beginseqno, mwseqno, bgheap)

	var purgedisk api.Index
	var nlevel int
	if bogn.dgmstate == 0 { // full set in memory
		purgedisk, nlevel = snap.disks[len(snap.disks)-1], len(snap.disks)-1
	} else {
		purgedisk, nlevel = bogn.pickwindupdisk()
	}
	nversion := bogn.nextdiskversion(nlevel)
	disksetts := bogn.settingstodisk()

	ids := []string{snap.mw.ID()}
	if purgedisk != nil {
		ids = append(ids, purgedisk.ID())
	}
	infof("%v dowindup: %v", bogn.logprefix, strings.Join(ids, " + "))

	// Finalize mw level, to catch up with tip.
	snap.finalizeindex(snap.mw)

	itere, uuid := snap.windupiterator(purgedisk), bogn.newuuid()
	appendid, valuelogs := bogn.indexvaluelogs([]api.Index{purgedisk})
	ndisk, err := bogn.builddiskstore(
		"dowindup", nlevel, nversion, uuid, "" /*flushunix*/, disksetts, itere,
		appendid, valuelogs, "windup", nil, /*appdata*/
	)
	if err != nil {
		return err
	}
	itere(true /*fin*/)

	bogn.addamplification(ndisk)

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

		fmsg := "%v dowindup: new snapshot %s windup on disk %v"
		infof(fmsg, snap.bogn.logprefix, head.attributes(), ndisk.ID())
	}()
	return nil
}

func compactticker(bogn *Bogn, compactorch chan []interface{}) {
	infof("%v tcompactor: starting...", bogn.logprefix)

	atomic.AddInt64(&bogn.nroutines, 1)
	defer func() {
		infof("%v tcompactor: stopped", bogn.logprefix)
		atomic.AddInt64(&bogn.nroutines, -1)
	}()

	ticker := time.NewTicker(Compacttick)
	for range ticker.C {
		posttick(bogn)
		select {
		case <-bogn.finch:
			return
		default:
		}
	}
}
