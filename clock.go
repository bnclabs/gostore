// vectorclock is similar to tlamport clock per vbucket
//	- {vbno, vbuuid, seqno}
//
// if len(vbnos) == 0, called the clock in reset-state, would sort before
// any other clock with len(vbnos) > 0.

package storage

type vectorclock struct {
	vbnos   []uint16
	vbuuids []uint64
	seqnos  []uint64
}

func newvectorclock(vbnos []uint16) *vectorclock {
	clock := &vectorclock{
		vbnos:   make([]uint16, len(vbnos)),
		vbuuids: make([]uint64, len(vbnos)),
		seqnos:  make([]uint64, len(vbnos)),
	}

	for i, vbno := range vbnos {
		clock.vbnos[i] = vbno
	}
	return clock
}

func (clock *vectorclock) reset() {
	clock.vbnos = clock.vbnos[:0]
}

func (clock *vectorclock) clone() *vectorclock {
	newclock := newvectorclock(clock.vbnos)
	for i, vbuuid := range clock.vbuuids {
		clock.vbuuids[i] = vbuuid
	}
	for i, seqno := range clock.seqnos {
		clock.seqnos[i] = seqno
	}
	return newclock
}
func (clock *vectorclock) setvbuuids(vbnos []uint16, vbuuids []uint64) {
	clock.vbnos = clock.vbnos[:len(vbnos)]
	for i, vbno := range vbnos {
		clock.vbnos[i] = vbno
		clock.vbuuids[i] = vbuuids[i]
	}
}

func (clock *vectorclock) setseqnos(vbnos []uint16, seqnos []uint64) {
	clock.vbnos = clock.vbnos[:len(vbnos)]
	for i, vbno := range vbnos {
		clock.vbnos[i] = vbno
		clock.seqnos[i] = seqnos[i]
	}
}

func (clock *vectorclock) updatevbuuids(vbnos []uint16, vbuuids []uint64) {
	if len(clock.vbnos) != cap(clock.vbnos) {
		panic("updatevbuuids(): methods allowed only on full clock-set")
	}
	for i, vbno := range vbnos {
		clock.vbuuids[vbno] = vbuuids[i]
	}
}

func (clock *vectorclock) updateseqnos(vbnos []uint16, seqnos []uint64) {
	if len(clock.vbnos) != cap(clock.vbnos) {
		panic("updateseqnos(): methods allowed only on full clock-set")
	}
	for i, vbno := range vbnos {
		clock.seqnos[vbno] = seqnos[i]
	}
}

func (clock *vectorclock) check(req *vectorclock) bool {
	if len(req.vbnos) == 0 { // reset-state
		return true
	}

	if len(clock.vbnos) == cap(clock.vbnos) {
		for i, vbno := range req.vbnos {
			if clock.vbuuids[vbno] != req.vbuuids[i] {
				return false
			}
		}
		return true
	} else {
		panic("check(): source clock should be full set")
	}
	return true
}

func (clock *vectorclock) lt(req *vectorclock) bool {
	if len(req.vbnos) == 0 { //reset-state
		return true
	}

	if len(clock.vbnos) == cap(clock.vbnos) {
		for i, vbno := range req.vbnos {
			if !(clock.seqnos[vbno] < req.seqnos[i]) {
				return false
			}
		}
		return true
	} else {
		panic("check(): source clock should be full set")
	}
}

func (clock *vectorclock) le(req *vectorclock) bool {
	if len(req.vbnos) == 0 { //reset-state
		return true
	}

	if len(clock.vbnos) == cap(clock.vbnos) {
		for i, vbno := range req.vbnos {
			if !(clock.seqnos[vbno] <= req.seqnos[i]) {
				return false
			}
		}
		return true
	} else {
		panic("check(): source clock should be full set")
	}
}

func (clock *vectorclock) gt(req *vectorclock) bool {
	if len(req.vbnos) == 0 { //reset-state
		return true
	}

	if len(clock.vbnos) == cap(clock.vbnos) {
		for i, vbno := range req.vbnos {
			if !(clock.seqnos[vbno] > req.seqnos[i]) {
				return false
			}
		}
		return true
	} else {
		panic("check(): source clock should be full set")
	}
}

func (clock *vectorclock) ge(req *vectorclock) bool {
	if len(req.vbnos) == 0 { //reset-state
		return true
	}

	if len(clock.vbnos) == cap(clock.vbnos) {
		for i, vbno := range req.vbnos {
			if !(clock.seqnos[vbno] >= req.seqnos[i]) {
				return false
			}
		}
		return true
	} else {
		panic("check(): source clock should be full set")
	}
}
