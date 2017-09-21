package llrb

import "time"

// go-routine to generate snapshots.
func housekeeper(mvcc *MVCC, interval time.Duration, finch chan struct{}) {
	tick := time.NewTicker(interval)
	defer tick.Stop()

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
