package storage

import "testing"
import "fmt"
import "math/rand"
import "encoding/binary"

var _ = fmt.Sprintf("dummy")

func TestVectorclockOrder(t *testing.T) {
	vc1 := newvectorclock(4)
	vbnos := []uint16{0, 1, 2, 3}
	seqnos := []uint64{1, 100, 200, 0}
	vbuuids := []uint64{0xeaa, 0xeab, 0xeac, 0xead}
	vc1.setseqnos(vbnos, seqnos)
	vc1.updateseqnos(vbnos, seqnos)
	vc1.updatevbuuids(vbnos, vbuuids)

	vc2, vc3, vc4 := vc1.clone(), vc1.clone(), vc1.clone()

	vbnos = []uint16{1, 3}
	nseqnos := []uint64{seqnos[1], seqnos[3]}
	nvbuuids := []uint64{vbuuids[1], vbuuids[3]}
	vc2.setseqnos(vbnos, nseqnos)
	vc2.updatevbuuids(vbnos, nvbuuids)

	vbnos = []uint16{0, 1, 3}
	nseqnos = []uint64{seqnos[0] - 1, seqnos[1], seqnos[3]}
	nvbuuids = []uint64{vbuuids[0], vbuuids[1], vbuuids[3]}
	vc3.setseqnos(vbnos, nseqnos)
	vc3.updatevbuuids(vbnos, nvbuuids)

	vbnos = []uint16{1, 3}
	nseqnos = []uint64{seqnos[1] + 1, seqnos[3] + 1}
	nvbuuids = []uint64{vbuuids[1], vbuuids[3]}
	vc4.setseqnos(vbnos, nseqnos)
	vc4.updatevbuuids(vbnos, nvbuuids)

	testcases := [][3]interface{}{
		// fullset (compare) subset
		[3]interface{}{"vc1.lt(vc2)", vc1.lt(vc2), false},
		[3]interface{}{"vc1.le(vc2)", vc1.le(vc2), false},
		[3]interface{}{"vc1.lt(vc4)", vc1.lt(vc4), false},
		[3]interface{}{"vc1.le(vc4)", vc1.le(vc4), false},
		[3]interface{}{"vc3.lt(vc2)", vc3.lt(vc2), false},
		[3]interface{}{"vc3.le(vc2)", vc3.le(vc2), true},
		// subset (compare) fullset
		[3]interface{}{"vc2.lt(vc1)", vc2.lt(vc1), false},
		[3]interface{}{"vc2.le(vc1)", vc2.le(vc1), true},
		[3]interface{}{"vc3.lt(vc1)", vc3.lt(vc1), false},
		[3]interface{}{"vc3.le(vc1)", vc3.le(vc1), true},
		[3]interface{}{"vc2.lt(vc3)", vc2.lt(vc3), false},
		[3]interface{}{"vc2.le(vc3)", vc2.le(vc3), true},
		[3]interface{}{"vc4.lt(vc2)", vc4.lt(vc2), false},
		[3]interface{}{"vc4.le(vc2)", vc4.le(vc2), false},
		[3]interface{}{"vc2.lt(vc4)", vc2.lt(vc4), true},
		[3]interface{}{"vc2.le(vc4)", vc2.le(vc4), true},
	}
	for _, tcase := range testcases {
		s, got, ref := tcase[0].(string), tcase[1].(bool), tcase[2].(bool)
		if ref != got {
			fmt.Println(vc2.crc64, vc2.seqnos)
			fmt.Println(vc4.crc64, vc4.seqnos)
			t.Errorf("%q expected %v, got %v", s, ref, got)
		}
	}

	// compare after changing crc
	vc4.updatevbuuids([]uint16{1}, []uint64{0xee})

	if vc2.lt(vc4) != false {
		t.Errorf("expected false, got true")
	} else if vc2.le(vc4) != false {
		t.Errorf("expected false, got true")
	} else if vc4.lt(vc2) != false {
		t.Errorf("expected false, got true")
	} else if vc4.le(vc2) != false {
		t.Errorf("expected false, got true")
	}
}

func TestVectorclockClone(t *testing.T) {
	vc1 := testcreatevc(8)
	vc2 := vc1.clone()
	if vc1.check(vc2) != true {
		t.Errorf("expected true, got false")
	} else if vc2.check(vc1) != true {
		t.Errorf("expected true, got false")
	}

	if vc1.le(vc2) != true {
		t.Errorf("expected true, got false")
	} else if vc1.lt(vc2) != false {
		t.Errorf("expected false, got true")
	} else if vc2.lt(vc1) != false {
		t.Errorf("expected false, got true")
	} else if vc2.le(vc1) != true {
		t.Errorf("expected true, got false")
	}
}

func TestVectorclockCheck(t *testing.T) {
	vc1 := testcreatevc(8)
	vc2 := vc1.clone()

	vc2.updatevbuuids([]uint16{1, 2}, []uint64{1, 4})
	if vc1.check(vc2) != false {
		t.Errorf("expected false, got true")
	} else if vc2.check(vc1) != false {
		t.Errorf("expected false, got true")
	}

	vc2.updatevbuuids([]uint16{0, 1, 2, 3, 4, 5, 6, 7}, vc1.vbuuids)
	if vc1.check(vc2) != true {
		t.Errorf("expected true, got false")
	} else if vc2.check(vc1) != true {
		t.Errorf("expected true, got false")
	}
}

func BenchmarkClockReset(b *testing.B) {
	clock := testcreatevc(1024)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		clock.reset()
	}
}

func BenchmarkClockHash(b *testing.B) {
	clock := testcreatevc(1024)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		clock.hash()
	}
}

func BenchmarkClockUVbuuids(b *testing.B) {
	clock := testcreatevc(1024)
	other := testcreatevc(1024)
	vbnos := testvbnolist(1024)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		clock.updatevbuuids(vbnos, other.vbuuids)
	}
}

func BenchmarkClockSSeqnos(b *testing.B) {
	clock := testcreatevc(1024)
	other := testcreatevc(1024)
	vbnos := testvbnolist(1024)

	seqnos := testseqnos2uint64(other.seqnos)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		clock.setseqnos(vbnos, seqnos)
	}
}

func BenchmarkClockSSeqnos10(b *testing.B) {
	clock := testcreatevc(1024)
	other := testcreatevc(1024)
	vbnos := testvbnolist(1024)

	seqnos := testseqnos2uint64(other.seqnos)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		clock.setseqnos(vbnos[:10], seqnos[:10])
	}
}

func BenchmarkClockUSeqno(b *testing.B) {
	clock := testcreatevc(1024)
	other := testcreatevc(1024)

	seqnos := testseqnos2uint64(other.seqnos)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		clock.updateseqno(1023, seqnos[1023])
	}
}

func BenchmarkClockUSeqnos(b *testing.B) {
	clock := testcreatevc(1024)
	other := testcreatevc(1024)
	vbnos := testvbnolist(1024)

	seqnos := testseqnos2uint64(other.seqnos)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		clock.updateseqnos(vbnos, seqnos)
	}
}

func BenchmarkClockLesse(b *testing.B) {
	clock := testcreatevc(1024)
	other := testcreatevc(1024)
	vbnos := testvbnolist(1024)

	seqnos := testseqnos2uint64(other.seqnos)

	clock.updateseqnos(vbnos, seqnos)
	clock.updatevbuuids(vbnos, other.vbuuids)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		clock.le(other)
	}
}

func testcreatevc(numvb int) *vectorclock {
	vbnos, vbuuids := make([]uint16, 0, numvb), make([]uint64, 0, numvb)
	seqnos := make([]uint64, 0, numvb)

	vc := newvectorclock(numvb)
	for i := 0; i < numvb; i++ {
		vbnos = append(vbnos, uint16(i))
		vbuuids = append(vbuuids, uint64(rand.Int63()))
		seqnos = append(seqnos, uint64(rand.Int63()))
	}
	vc.setseqnos(vbnos, seqnos)
	vc.updatevbuuids(vbnos, vbuuids)
	return vc
}

func testvbnolist(numvb int) []uint16 {
	vbnos := make([]uint16, 1024)
	for i := 0; i < numvb; i++ {
		vbnos[i] = uint16(i)
	}
	return vbnos
}

func testseqnos2uint64(seqnosb []byte) []uint64 {
	seqnos := make([]uint64, 0)
	for i := 0; i < len(seqnosb); i += 8 {
		seqnos = append(seqnos, binary.BigEndian.Uint64(seqnosb[i:i+8]))
	}
	return seqnos
}
