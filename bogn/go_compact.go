package bogn

import "fmt"
import "time"
import "sync/atomic"
import "runtime/debug"

import "github.com/prataprc/golog"
import "github.com/prataprc/gostore/api"
import "github.com/prataprc/gostore/lib"
import "github.com/prataprc/gostore/bubt"

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
	}()

	ticker := time.NewTicker(Compacttick)
	docompact := makecompactor(bogn)
	var respch chan [3]api.Index
	var errch chan error

	// atmost two concurrent compaction can run.
	// a. compacting data in memory with latest level on disk (aka flush).
	// b. compacting data betwen two disk levels.
loop:
	for range ticker.C {
		if bogn.isclosed() {
			break loop
		}
		if err := docompact(); err != nil {
			panic(err)

		} else if respch == nil {
			respch, errch = startdisk(bogn)

		} else {
			select {
			case resp := <-respch:
				if err := findisk(bogn, resp); err != nil {
					panic(err)
				}
				respch, errch = nil, nil

			case err := <-errch:
				panic(err)

			default:
			}
		}
	}
}

func makecompactor(bogn *Bogn) func() error {
	mwcap := float64(bogn.memcapacity)
	// adaptive threshold.
	mwthreshold := int64(mwcap * .9) // start with 90% of configured capacity
	if bogn.workingset {             // start with 30% of configured capacity
		mwthreshold = int64(mwcap * .3)
	} else if bogn.dgm { // start with 50% of configured capacity
		mwthreshold = int64(mwcap * .5)
	}

	return func() error {
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
			err := doflush(bogn)
			// adaptive threshold
			snap := bogn.currsnapshot()
			bgheap := float64(snap.memheap())
			mwthreshold += int64((mwcap - float64(mwthreshold)) - bgheap)
			return err
		}
		return nil
	}
}

// called only when full data set in memory.
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
		head.yget, head.next = head.mw.Get, snap
		bogn.setheadsnapshot(head)

		snap.addtopurge(snap.disklevels([]api.Index{})...)
		log.Infof("%v compact persisted %v", snap.bogn.logprefix, ndisk.ID())
	}()

	return nil
}

func doflush(bogn *Bogn) (err error) {
	var head0, head1 *snapshot

	snap := bogn.currsnapshot()
	level, version, disk := bogn.pickflushdisk()
	if level < 0 {
		panic("impossible situation")
	}

	func() {
		bogn.snaprw.Lock()
		defer bogn.snaprw.Unlock()

		var mw api.Index
		if mw, err = bogn.newmemstore("mw", snap.mwseqno()); err != nil {
			panic(err) // should never happen
		}
		head1 = newsnapshot(bogn, mw, snap.mw, snap.mc, snap.disks)
		head1.next = snap
		bogn.setheadsnapshot(head1)

		snap.refer()
		snap.addtopurge(disk)
		log.Infof("%v compact flush ...", snap.bogn.logprefix)
	}()

	now, iter := time.Now(), head1.flushiterator(disk)
	ndisk, count, err := bogn.builddiskstore(level, version, iter)
	if err != nil {
		return err
	}
	fmsg := "%v took %v to build %v with %v entries\n"
	log.Infof(fmsg, bogn.logprefix, time.Since(now), ndisk.ID(), count)

	var disks [16]api.Index
	copy(disks[:], head1.disks[:])
	disks[level] = ndisk

	func() {
		bogn.snaprw.Lock()
		defer bogn.snaprw.Unlock()

		var mc api.Index

		if bogn.workingset {
			if mc, err = bogn.newmemstore("mc", 0); err != nil {
				panic(err) // should never happen
			}
		}

		// TODO: between head1 and head0 newly cached entries would be
		// lost.
		head0 = newsnapshot(bogn, head1.mw, nil, mc, disks)
		head0.next = head1
		bogn.setheadsnapshot(head0)

		head1.addtopurge(head1.mr, head1.mc)
		log.Infof("%v compact flush ... ok", snap.bogn.logprefix, ndisk.ID())
	}()

	return nil
}

func startdisk(bogn *Bogn) (chan [3]api.Index, chan error) {
	snap := bogn.currsnapshot()
	disks := snap.disklevels([]api.Index{})
	for i := 0; i < len(disks)-1; i++ {
		disk0, disk1 := disks[i], disks[i+1]
		footprint0 := float64(disk0.(*bubt.Snapshot).Footprint())
		footprint1 := float64(disk1.(*bubt.Snapshot).Footprint())
		if (footprint0 / footprint1) < bogn.ratio {
			continue
		}

		respch, errch := make(chan [3]api.Index, 1), make(chan error, 1)
		iter := snap.compactiterator(disk0, disk1)

		go func() {
			fmsg := "%v compacting %q + %q ..."
			log.Infof(fmsg, bogn.logprefix, disk0.ID(), disk1.ID())
			level, version, _ := bogn.path2level(disk0.ID())

			now := time.Now()
			ndisk, count, err := bogn.builddiskstore(level, version+1, iter)
			if err != nil {
				errch <- err
				return
			}
			fmsg = "%v took %v to build %v with %v entries\n"
			log.Infof(fmsg, bogn.logprefix, time.Since(now), ndisk.ID(), count)

			respch <- [3]api.Index{ndisk, disk0, disk1}
		}()
		return respch, errch
	}
	return nil, nil
}

func findisk(bogn *Bogn, resp [3]api.Index) error {
	ndisk, disk0, disk1 := resp[0], resp[1], resp[2]
	snap := bogn.currsnapshot()
	disks := snap.disklevels([]api.Index{})

	for i := 0; i < len(disks)-1; i++ {
		id0, id1 := disks[i].ID(), disks[i+1].ID()
		if disk0.ID() == id0 && disk1.ID() == id1 {
			var disks [16]api.Index
			copy(disks[:], snap.disks[:])
			level, _, _ := bogn.path2level(ndisk.ID())
			disks[level] = ndisk
			func() {
				bogn.snaprw.Lock()
				defer bogn.snaprw.Unlock()
				head := newsnapshot(bogn, snap.mw, snap.mr, snap.mc, disks)
				head.next = snap
				bogn.setheadsnapshot(head)
				snap.addtopurge(disk0, disk1)
				fmsg := "%v compact disk"
				log.Infof(fmsg, snap.bogn.logprefix, ndisk.ID())
			}()
			return nil
		}
	}
	// Looks like the memory-disk compaction has touched disk0 or disk1.
	func() {
		bogn.snaprw.Lock()
		defer bogn.snaprw.Unlock()
		snap.addtopurge(ndisk)
		log.Infof("%v compact abandon", snap.bogn.logprefix, ndisk.ID())
	}()
	return nil
}
