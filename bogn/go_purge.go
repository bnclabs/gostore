package bogn

import "time"
import "sync/atomic"
import "runtime/debug"

import "github.com/prataprc/golog"
import "github.com/prataprc/gostore/lib"

func purger(bogn *Bogn) {
	atomic.AddInt64(&bogn.nroutines, 1)
	log.Infof("%v starting purger", bogn.logprefix)
	defer func() {
		if r := recover(); r != nil {
			log.Errorf("%v purger crashed %v", bogn.logprefix, r)
			log.Errorf("\n%s", lib.GetStacktrace(2, debug.Stack()))
		} else {
			log.Infof("%v stopped purger", bogn.logprefix)
		}
		atomic.AddInt64(&bogn.nroutines, -1)
	}()

	ticker := time.NewTicker(Compacttick)
loop:
	for range ticker.C {
		snap := bogn.currsnapshot()
		if snap != nil && purgesnapshot(snap.next) {
			snap.next = nil
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
	if purgesnapshot(snap.next) {
		snap.next = nil
		snap.setpurge()
		if snap.getref() == 0 {
			// all older snapshots are purged,
			// and this snapshot is not refered by anyone.
			for _, index := range snap.purgeindexes {
				index.Close()
				index.Destroy()
				log.Infof("%v purged %q", snap.bogn.logprefix, index.ID())
			}
			snap.close()
			return true
		}
		snap.clearpurge()
	}
	return false
}