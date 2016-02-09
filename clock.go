// vectorclock is similar to lamport clock per vbucket
//	- {vbno, vbuuid, seqno}
//
// if len(vbnos) == 0, called the clock in reset-state, would sort before
// any other clock with len(vbnos) > 0.
//
// create a new clock and keep re-using them:
//
//		clock := newvectorclock(1024) // always create them with max-vbuckets
//
// initialize a clock with new set of {vbno,vbuuid,seqno}:
//
//      vbnos, vbuuids := []uint16{0,1,2,3}, []uint64{11,22,33,44}
//      seqnos := []uint16{100,200,300,400}
//		clock := newvectorclock(4)
//		clock.setseqnos(vbnos, seqnos)
//		clock.updatevbuuids(vbnos, vbuuids)
//
// reset the clock:
// clock timestamp is empty, but memory is not freed and vbuuid remains the
// same.
//
//		clock.reset()
//
// updating a clock:
//
//		clock.updateseqnos([]uint16{3,4}, []uint64{}{301,401})
//		clock.updateseqno(3, 302)
//		clock.updatevbuuids([]uint16{1,2}, []uint64{12,23})
//
// comparing clock order:
//
// In this version of the vector-clock we can cannot maintain a strict
// ordering of the clocks, instead the following set of rules are defined:
//
//	   * only lt (less-than) and le (less than or equal) is defined.
//	   * clock.lt(other) is true, clock is gauraunteed to be before other.
//	   * clock.le(other) is true, clock is gaurannteed to be before or equal as
//		 other.
//	   * clock.lt(other) and clock.le(other) returns false, it does not mean
//		 anything.
//	   * if other is subset of clock - lt, and le always return false.

package storage

import "hash"
import "bytes"
import "hash/crc64"
import "encoding/binary"

type vectorclock struct {
	vbuuids []uint64
	seqnos  []byte
	crc64   uint64 // 64 bit hash of all vbuuids in the clock
	hasher  hash.Hash64
}

var resetseqnos = make([]byte, 2024*8) // TODO: avoid magic numbers

func newvectorclock(numvb int) *vectorclock {
	clock := &vectorclock{
		vbuuids: make([]uint64, numvb),
		seqnos:  make([]byte, numvb*8),
		crc64:   0,
		hasher:  crc64.New(crc64.MakeTable(crc64.ECMA)),
	}

	return clock
}

func (clock *vectorclock) reset() *vectorclock {
	copy(clock.seqnos, resetseqnos)
	return clock
}

func (clock *vectorclock) hash() uint64 {
	var scratch [8]byte
	clock.hasher.Reset()
	for _, vbuuid := range clock.vbuuids {
		binary.BigEndian.PutUint64(scratch[:], vbuuid)
		clock.hasher.Write(scratch[:])
	}
	clock.crc64 = clock.hasher.Sum64()
	return clock.crc64
}

func (clock *vectorclock) setseqnos(vbnos []uint16, seqnos []uint64) {
	clock = clock.reset()
	for i, vbno := range vbnos {
		binary.BigEndian.PutUint64(clock.seqnos[vbno*8:], seqnos[i])
	}
}

func (clock *vectorclock) updatevbuuids(vbnos []uint16, vbuuids []uint64) {
	for i, vbno := range vbnos {
		clock.vbuuids[vbno] = vbuuids[i]
	}
	clock.hash()
}

func (clock *vectorclock) updateseqno(vbno uint16, seqno uint64) {
	binary.BigEndian.PutUint64(clock.seqnos[vbno*8:], seqno)
}

func (clock *vectorclock) updateseqnos(vbnos []uint16, seqnos []uint64) {
	for i, vbno := range vbnos {
		clock.updateseqno(vbno, seqnos[i])
	}
}

func (clock *vectorclock) clone() *vectorclock {
	newclock := newvectorclock(len(clock.vbuuids))
	copy(newclock.vbuuids, clock.vbuuids)
	copy(newclock.seqnos, clock.seqnos)
	newclock.hash()
	return newclock
}

func (clock *vectorclock) check(other *vectorclock) bool {
	return clock.crc64 == other.crc64
}

func (clock *vectorclock) lt(other *vectorclock) bool {
	if clock.crc64 != other.crc64 {
		return false
	}

	for i := 0; i < len(other.seqnos); i += 8 {
		x := binary.BigEndian.Uint64(clock.seqnos[i : i+8])
		y := binary.BigEndian.Uint64(other.seqnos[i : i+8])
		if x == 0 {
			continue
		} else if !(x < y) {
			return false
		}
	}
	return true
}

func (clock *vectorclock) le(other *vectorclock) bool {
	if clock.crc64 != other.crc64 {
		return false
	}
	return bytes.Compare(clock.seqnos, other.seqnos) < 1
}
