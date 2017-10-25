package bogn

import "fmt"
import "time"
import "sync/atomic"
import "runtime/debug"

import "github.com/prataprc/golog"
import "github.com/prataprc/gostore/api"
import "github.com/prataprc/gostore/lib"
import "github.com/prataprc/gostore/bubt"

var Compacttick = time.Duration(1 * time.Second)

func compactor(bogn *Bogn, flushperiod time.Duration) {
	atomic.AddInt64(&bogn.nroutines, 1)
	log.Infof("%v starting compactor", bogn.logprefix)
	defer func() {
		close(bogn.purgech)
		if r := recover(); r != nil {
			log.Errorf("%v compactor crashed %v", bogn.logprefix, r)
			log.Errorf("\n%s", lib.GetStacktrace(2, debug.Stack()))
		} else {
			log.Infof("%v stopped compactor", bogn.logprefix)
		}
		atomic.AddInt64(&bogn.nroutines, -1)
	}()

	ticker, mwcap := time.NewTicker(Compacttick), float64(bogn.memcapacity)

	// 90% of configured capacity
	mwthreshold := int64(mwcap * .9)
	if bogn.workingset {
		// 30% of configured capacity
		mwthreshold = int64(mwcap * .3)
	} else if bogn.dgm {
		// 50% of configured capacity
		mwthreshold = int64(mwcap * .5)
	}

	tmpersist := time.Now()

loop:
	for range ticker.C {
		if bogn.isclosed() {
			break loop
		}

		snap := bogn.currsnapshot()
		overflow := snap.memheap() > mwthreshold

		if bogn.dgm == false {
			if overflow == false {
				if time.Since(tmpersist) > flushperiod {
					tmpersist = time.Now() // reload
					if err := dopersist(bogn); err != nil {
						panic(err)
					}
				}
				continue loop
			} else {
				bogn.dgm = true
				mwthreshold = int64(mwcap * .5)
			}
		}

		if overflow {
			if err := doflush(bogn); err != nil {
				panic(err)
			}
			snap := bogn.currsnapshot()
			mwthreshold += int64(mwcap - float64(snap.memheap()))
			continue
		}

		// compact disk levels
		disks := snap.disklevels([]api.Index{})
		for i := 0; i < len(disks)-1; i++ {
			disk0, disk1 := disks[i], disks[i+1]
			footprint0 := float64(disk0.(*bubt.Snapshot).Footprint())
			footprint1 := float64(disk1.(*bubt.Snapshot).Footprint())
			if (footprint0 / footprint1) > bogn.ratio {
				err := docompact(bogn, disk0, disk1)
				if err != nil {
					panic(err)
				}
			}
		}

	}
}

func dopersist(bogn *Bogn) (err error) {
	snap := bogn.currsnapshot()

	lvl, level, version := -1, len(snap.disks)-1, 1
	if disk := snap.disks[level]; disk != nil {
		if lvl, version = bogn.path2level(disk.ID()); lvl != level {
			panic(fmt.Errorf("mismatch in level %v != %v", level, lvl))
		}
	}

	now, iter := time.Now(), snap.persistiterator()
	ndisk, count, err := bogn.builddiskstore(level, version, iter)
	if err != nil {
		return err
	}
	fmsg := "%v Took %v to build %v with %v entries\n"
	log.Infof(fmsg, bogn.logprefix, time.Since(now), ndisk.ID(), count)

	func() {
		var disks [16]api.Index

		bogn.snaprw.Lock()
		disks[level] = ndisk
		head := newsnapshot(bogn, snap.mw, nil, nil, disks)
		head.yget = head.mw.Get
		bogn.setheadsnapshot(head)
		bogn.snaprw.Unlock()

		for _, disk := range snap.disklevels([]api.Index{}) {
			bogn.purgeindex(disk)
		}
	}()

	return nil
}

func doflush(bogn *Bogn) (err error) {
	var level, version int

	snap := bogn.currsnapshot()
	level, disk := snap.latestlevel()
	if level < 0 {
		level, version = len(snap.disks)-1, 1

	} else if disk != nil {
		_, version = bogn.path2level(disk.ID())
		footprint := float64(disk.(*bubt.Snapshot).Footprint())
		if (float64(snap.memheap()) / footprint) > bogn.ratio {
			level, disk = level-1, nil
		}
	}
	if level < 0 {
		panic("impossible situation")
	}

	now, iter := time.Now(), snap.flushiterator(disk)
	ndisk, count, err := bogn.builddiskstore(level, version, iter)
	if err != nil {
		return err
	}
	fmsg := "%v Took %v to build %v with %v entries\n"
	log.Infof(fmsg, bogn.logprefix, time.Since(now), ndisk.ID(), count)

	func() {
		var disks [16]api.Index
		var mw, mc api.Index

		copy(disks[:], snap.disks[:])
		disks[level] = ndisk
		if bogn.workingset {
			mc, err = bogn.newmemstore("mc", nil)
			if err != nil {
				panic(err) // should never happen
			}
		}
		mw, err = bogn.newmemstore("mw", snap.mw)
		if err != nil {
			panic(err) // should never happen
		}

		bogn.snaprw.Lock()
		head := newsnapshot(bogn, mw, snap.mw, mc, disks)
		bogn.setheadsnapshot(head)
		bogn.snaprw.Unlock()
		bogn.purgeindex(snap.mr)
		bogn.purgeindex(snap.mc)
		bogn.purgeindex(disk)
	}()

	return nil
}

func docompact(bogn *Bogn, disk0, disk1 api.Index) (err error) {
	snap := bogn.currsnapshot()
	level, version := bogn.path2level(disk0.ID())
	version++
	iter := snap.compactiterator(disk0, disk1)

	now := time.Now()
	ndisk, count, err := bogn.builddiskstore(level, version, iter)
	if err != nil {
		return err
	}
	fmsg := "%v Took %v to build %v with %v entries\n"
	log.Infof(fmsg, bogn.logprefix, time.Since(now), ndisk.ID(), count)

	func() {
		var disks [16]api.Index
		copy(disks[:], snap.disks[:])
		disks[level] = ndisk

		bogn.snaprw.Lock()
		head := newsnapshot(bogn, snap.mw, snap.mr, snap.mc, disks)
		bogn.setheadsnapshot(head)
		bogn.snaprw.Unlock()
		bogn.purgeindex(disk0)
		bogn.purgeindex(disk1)
	}()
	return nil
}
