package storage

import "testing"
import "math/rand"
import "unsafe"
import "fmt"

var _ = fmt.Sprintf("dummy")

func TestBlocksizes(t *testing.T) {
	minblock, maxblock := int64(96), int64(1024*1024*1024)
	ref := []int64{
		96, 128, 160, 192, 224, 256, 288, 320, 384, 448, 512, 576, 640,
		736, 832, 928, 1056, 1184, 1344, 1536, 1728, 1920, 2208, 2496,
		2784, 3168, 3552, 4032, 4608, 5280, 6048, 6912, 7872, 9024, 10368,
		11904, 13632, 15648, 17952, 20640, 23712, 27264, 31296, 35904, 41280,
		47424, 54528, 62688, 72000, 82752, 95136, 109344, 125664, 144480,
		166080, 190944, 219552, 252480, 290304, 333792, 383808, 441312,
		507456, 583488, 670944, 771552, 887232, 1020288, 1173312, 1349280,
		1551648, 1784352, 2052000, 2359776, 2713728, 3120768, 3588864,
		4127136, 4746144, 5457984, 6276672, 7218144, 8300832, 9545952,
		10977792, 12624384, 14517984, 16695648, 19199904, 22079808, 25391712,
		29200416, 33580416, 38617440, 44409984, 51071424, 58732128, 67541856,
		77673120, 89324064, 102722592, 118130976, 135850560, 156228096,
		179662272, 206611584, 237603264, 273243744, 314230272, 361364736,
		415569408, 477904800, 549590496, 632029056, 726833376, 835858368,
		961237056, 1073741824}
	sizes := Blocksizes(minblock, maxblock)
	if len(sizes) != len(ref) {
		t.Errorf("expected %v, got %v", len(ref), len(sizes))
	}
	for i, v := range ref {
		if sizes[i] != v {
			t.Errorf("expected %v, got %v - off %v", v, sizes[i], i)
		} else if (sizes[i] % 32) > 0 {
			t.Errorf("expected multiple of 32, got %v", sizes[i])
		}
	}

	// test panic
	fn := func(minblock, maxblock int64) {
		defer func() {
			if recover() == nil {
				t.Errorf("expected panic")
			}
		}()
		Blocksizes(minblock, maxblock)
	}
	fn(1024, 96)
	fn(100, 1024)
	fn(96, 1000)
}

func TestSuitableSize(t *testing.T) {
	minblock, maxblock := int64(96), int64(1024*1024)
	sizes := Blocksizes(minblock, maxblock)
	x := sizes[1]
	for _, y := range sizes[1:] {
		for i := x + 1; i <= y; i++ {
			if z := SuitableSize(sizes, i); z != y {
				t.Errorf("for %v expected %v, got %v", i, y, z)
			}
		}
		x = y
	}
}

func TestNewmarena(t *testing.T) {
	minblock, maxblock := int64(96), int64(1024*1024*1024)
	capacity, pcapacity := int64(1024*1024*1024*1024), int64(1024*1024)
	marena := newmemarena(minblock, maxblock, capacity, pcapacity)
	if x := len(marena.blocksizes); x != 118 {
		t.Errorf("expected %v, got %v", 118, x)
	}
	if x, y := len(marena.blocksizes), len(marena.mpools); x != y {
		t.Errorf("expected %v, got %v", x, y)
	}
	marena.release()

	// panic cases
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("expected panic")
			}
		}()
		min, max := int64(0), int64(0x1234567812344556)
		newmemarena(min, max, capacity, pcapacity)
	}()
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("expected panic")
			}
		}()
		min, max := int64(0), int64(359399435061660672)
		newmemarena(min, max, capacity, pcapacity)
	}()
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("expected panic")
			}
		}()
		newmemarena(int64(32), 1024, maxarenasize+1, pcapacity)
	}()
}

func TestArenaAlloc(t *testing.T) {
	minblock, maxblock := int64(96), int64(1024)
	capacity, pcapacity := int64(1024*1024*1024), int64(1024*1024)
	marena := newmemarena(minblock, maxblock, capacity, pcapacity)
	ptrs, mpools := make([]unsafe.Pointer, 1024), make([]*mempool, 1024)
	for i := 0; i < 1024; i++ {
		ptrs[i], mpools[i] = marena.alloc(1024)
		if ptrs[i] == nil || mpools[i] == nil {
			t.Errorf("unexpected allocation failure")
		}
	}
	if x := marena.available(); x != 1072693248 {
		t.Errorf("expected %v, got %v", 1072693248, x)
	} else if x, y := marena.allocated(), int64(1024*1024); x != y {
		t.Errorf("expected %v, got %v", x, y)
	}
	// panic case
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("expected panic")
			}
		}()
		marena.alloc(maxblock + 1)
	}()
	marena.release()
}

func TestArenaMemory(t *testing.T) {
	minblock, maxblock := int64(96), int64(1024*1024*1024)
	capacity, pcapacity := int64(1024*1024*1024*1024), int64(1024*1024)
	marena := newmemarena(minblock, maxblock, capacity, pcapacity)
	if x, y := marena.memory(); x != 2112 {
		t.Errorf("expected %v, got %v", 2112, x)
	} else if y != 0 {
		t.Errorf("expected %v, got %v", 0, y)
	}
	marena.release()
}

func BenchmarkBlocksizes(b *testing.B) {
	minblock, maxblock := int64(96), int64(1024*1024*0124*1024)
	for i := 0; i < b.N; i++ {
		Blocksizes(minblock, maxblock)
	}
}

func BenchmarkSuitableSize(b *testing.B) {
	minblock, maxblock := int64(96), int64(1024*1024*1024*10)
	sizes := Blocksizes(minblock, maxblock)
	rsizes := make([]int64, b.N)

	for i := 0; i < b.N; i++ {
		rsizes[i] = int64(rand.Intn(int(maxblock)))
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		SuitableSize(sizes, rsizes[i])
	}
}

func BenchmarkNewarena(b *testing.B) {
	minblock, maxblock := int64(96), int64(1024*1024*1024)
	capacity, pcapacity := int64(1024*1024*1024*1024), int64(1024*1024)
	for i := 0; i < b.N; i++ {
		newmemarena(minblock, maxblock, capacity, pcapacity)
	}
}

func BenchmarkArenaAlloc(b *testing.B) {
	minblock, maxblock := int64(96), int64(1024*1024*1024)
	capacity, pcapacity := int64(1024*1024*1024*1024), int64(1024*1024)
	marena := newmemarena(minblock, maxblock, capacity, pcapacity)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if ptr, mpool := marena.alloc(96); ptr == nil || mpool == nil {
			b.Errorf("unexpected failure in allocation")
		}
	}
}

func BenchmarkArenaMemory(b *testing.B) {
	minblock, maxblock := int64(96), int64(1024*1024*1024)
	capacity, pcapacity := int64(3*1024*1024*1024), int64(1024*1024)
	marena := newmemarena(minblock, maxblock, capacity, pcapacity)
	for i := 0; i < 1024*1024; i++ {
		marena.alloc(int64(rand.Intn(2048)))
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		marena.memory()
	}
}

func BenchmarkArenaAllocated(b *testing.B) {
	minblock, maxblock := int64(96), int64(1024*1024*1024)
	capacity, pcapacity := int64(3*1024*1024*1024), int64(1024*1024)
	marena := newmemarena(minblock, maxblock, capacity, pcapacity)
	for i := 0; i < 1024*1024; i++ {
		marena.alloc(int64(rand.Intn(2048)))
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		marena.allocated()
	}
}
