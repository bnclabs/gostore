package llrb

import "time"

// go-routine to generate snapshots.
func housekeeper(
	mvcc *MVCC, interval int64, triggch chan bool, finch chan struct{}) {

	tick := time.NewTicker(time.Duration(interval) * time.Millisecond)
	defer tick.Stop()

loop:
	for {
		select {
		case <-tick.C:
		case <-triggch:
		}
		select { // break out if writer has exited
		case <-finch:
			break loop
		default:
		}
		mvcc.makesnapshot()
	}
}
