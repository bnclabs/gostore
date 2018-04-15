package bogn

import "time"
import "sync/atomic"
import "runtime/debug"

import "github.com/bnclabs/gostore/lib"

func purger(bogn *Bogn) {
	infof("%v rpurger: starting ...", bogn.logprefix)

	defer func() {
		if r := recover(); r != nil {
			errorf("%v purger crashed %v", bogn.logprefix, r)
			errorf("\n%s", lib.GetStacktrace(2, debug.Stack()))
		} else {
			infof("%v rpurger: stopped ", bogn.logprefix)
		}
		atomic.AddInt64(&bogn.nroutines, -1)
	}()

	atomic.AddInt64(&bogn.nroutines, 1)
	ticker := time.NewTicker(Compacttick)
loop:
	for range ticker.C {
		snap := bogn.currsnapshot()
		next := (*snapshot)(atomic.LoadPointer(&snap.next))
		if snap != nil && purgesnapshot(next) {
			atomic.StorePointer(&snap.next, nil)
		}
		select {
		case <-bogn.finch:
			break loop
		default:
		}
	}
}

func purgesnapshot(snap *snapshot) bool {
	if snap == nil {
		return true
	}
	next := (*snapshot)(atomic.LoadPointer(&snap.next))
	if purgesnapshot(next) {
		atomic.StorePointer(&snap.next, nil)
		snap.trypurge()
		if snap.getref() == 0 {
			// all older snapshots are purged,
			// and this snapshot is not referred by anyone.

			// first close the disk snapshot, this shall dereference the
			// snapshot.
			for _, index := range snap.purgeindexes {
				index.Close()
			}
			// then destroy
			for _, index := range snap.purgeindexes {
				index.Destroy()
				fmsg := "%v rpurger: purged %q in snapshot %v"
				infof(fmsg, snap.bogn.logprefix, index.ID(), snap.id)
			}
			snap.close()
			return true
		}
		snap.clearpurge()
	}
	return false
}
