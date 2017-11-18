package llrb

import "time"
import "sync/atomic"

// go-routine to generate snapshots.
func housekeeper(mvcc *MVCC, interval time.Duration, finch chan struct{}) {
	atomic.AddInt64(&mvcc.n_routines, 1)
	tick := time.NewTicker(interval)
	defer func() {
		tick.Stop()
		atomic.AddInt64(&mvcc.n_routines, -1)
	}()

loop:
	for {
		<-tick.C
		select { // break out if writer has exited
		case <-finch:
			break loop
		default:
		}
		mvcc.makesnapshot()
	}
}
