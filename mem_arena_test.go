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
		736, 844, 970, 1114, 1279, 1468, 1687, 1939, 2227, 2560, 2944,
		3385, 3892, 4474, 5143, 5914, 6799, 7816, 8986, 10333, 11881,
		13663, 15712, 18067, 20776, 23890, 27472, 31591, 36328, 41776,
		48040, 55246, 63532, 73060, 84019, 96619, 111109, 127774,
		146938, 168976, 194320, 223468, 256987, 295534, 339862, 390841,
		449467, 516886, 594418, 683578, 786112, 904027, 1039630, 1195573,
		1374907, 1581142, 1818313, 2091058, 2404714, 2765419, 3180229,
		3657262, 4205851, 4836727, 5562235, 6396568, 7356052, 8459458,
		9728374, 11187628, 12865771, 14795635, 17014978, 19567222,
		22502305, 25877650, 29759296, 34223188, 39356665, 45260164,
		52049188, 59856565, 68835049, 79160305, 91034350, 104689501,
		120392926, 138451864, 159219643, 183102589, 210567976,
		242153170, 278476144, 320247565, 368284699, 423527401, 487056511,
		560114986, 644132233, 740752066, 851864875, 979644604, maxblock}
	sizes := Blocksizes(minblock, maxblock)
	if len(sizes) != len(ref) {
		t.Errorf("expected %v, got %v", len(ref), len(sizes))
	}
	for i, v := range ref {
		if sizes[i] != v {
			t.Errorf("expected %v, got %v - off %v", v, sizes[i], i)
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
	if x := len(marena.blocksizes); x != 116 {
		t.Errorf("expected %v, got %v", 116, x)
	} else if x, y := len(marena.blocksizes), len(marena.mpools); x != y {
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
