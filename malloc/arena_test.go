package malloc

import "fmt"
import "testing"
import "unsafe"
import "sync"
import "reflect"
import "math/rand"

var _ = fmt.Sprintf("dummy")

func TestNewmarena(t *testing.T) {
	capacity := int64(10 * 1024 * 1024)
	marena := NewArena(capacity, "flist")
	if x := len(marena.slabs); x != 463 {
		t.Errorf("expected %v, got %v", 463, x)
	}
	if x, y := len(marena.slabs), len(marena.mpools); x != y {
		t.Errorf("expected %v, got %v", x, y)
	}
	marena.Release()

	// panic cases
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("expected panic")
			}
		}()
		capacity := int64(10 * 1024 * 1024)
		NewArena(capacity, "fbit")
	}()
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("expected panic")
			}
		}()
		capacity := Maxarenasize + 1
		NewArena(capacity, "flist")
	}()
}

func TestArenaAlloc(t *testing.T) {
	capacity := int64(10 * 1024 * 1024)
	marena := NewArena(capacity, "flist")
	ptrs := make([]unsafe.Pointer, 1024)
	for i := 0; i < 1024; i++ {
		ptrs[i] = marena.Alloc(1024)
		if ptrs[i] == nil {
			t.Errorf("unexpected allocation failure")
		}
	}
	capacity, heap, alloc, overhead := marena.Info()
	if capacity != 10485760 {
		t.Errorf("unexpected capacity %v", capacity)
	} else if heap != 1099872 {
		t.Errorf("unexpected heap %v", heap)
	} else if alloc != 1097728 {
		t.Errorf("unexpected alloc %v", alloc)
	} else if overhead != 29856 {
		t.Errorf("unexpected overhead %v", overhead)
	}

	if slabs, uzs := marena.Utilization(); len(slabs) != 1 {
		t.Errorf("unexpected %v", len(slabs))
	} else if slabs[0] != 1072 {
		t.Errorf("unexpected %v", slabs[0])
	} else if len(uzs) != 1 {
		t.Errorf("unexpected %v", len(uzs))
	} else if uzs[0] < 97 {
		t.Errorf("unexpected %v", uzs[0])
	}

	// panic case
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("expected panic")
			}
		}()
		marena.Alloc(marena.maxslab + 1)
	}()
	marena.Release()
}

func TestArenaInfo(t *testing.T) {
	capacity := int64(10 * 1024 * 1024)
	marena := NewArena(capacity, "flist")
	_, heap, _, overhead := marena.Info()
	if overhead != 12072 {
		t.Errorf("unexpected overhead %v", overhead)
	} else if heap != 0 {
		t.Errorf("unexpected overhead %v", heap)
	}
	marena.Release()
}

func TestArenaMaxchunks(t *testing.T) {
	// with capacity 1M
	capacity := int64(1024 * 1024)
	marena := NewArena(capacity, "flist")
	ref := [6]int64{20, 10, 1, 1, 1, -1}
	if !reflect.DeepEqual(ref, marena.maxchunks) {
		t.Errorf("expected %v, got %v", ref, marena.maxchunks)
	}
	// with capacity 100M
	capacity = int64(100 * 1024 * 1024)
	marena = NewArena(capacity, "flist")
	ref = [6]int64{2048, 1024, 64, 8, 1, 1}
	if !reflect.DeepEqual(ref, marena.maxchunks) {
		t.Errorf("expected %v, got %v", ref, marena.maxchunks)
	}
	// with capacity 1T
	capacity = int64(1024 * 1024 * 1024 * 1024)
	marena = NewArena(capacity, "flist")
	ref = [6]int64{20480, 20480, 20480, 20480, 10485, 655}
	if !reflect.DeepEqual(ref, marena.maxchunks) {
		t.Errorf("expected %v, got %v", ref, marena.maxchunks)
	}
}

func TestNumchunks(t *testing.T) {
	capacity := int64(1024 * 1024 * 1024 * 1024)
	marena := NewArena(capacity, "flist")
	// 100 byte
	out := []int64{}
	for npools := 0; npools < 12; npools++ {
		out = append(out, marena.adaptiveNumchunks(100, int64(npools)))
	}
	ref := []int64{
		64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 20480, 20480, 20480,
	}
	if !reflect.DeepEqual(out, ref) {
		t.Errorf("expected %v, got %v", ref, out)
	}

	// 800 byte
	out = []int64{}
	for npools := 0; npools < 12; npools++ {
		out = append(out, marena.adaptiveNumchunks(800, int64(npools)))
	}
	ref = []int64{
		64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 20480, 20480, 20480,
	}
	if !reflect.DeepEqual(out, ref) {
		t.Errorf("expected %v, got %v", ref, out)
	}

	// 8000 byte
	out = []int64{}
	for npools := 0; npools < 12; npools++ {
		out = append(out, marena.adaptiveNumchunks(8000, int64(npools)))
	}
	ref = []int64{
		64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 20480, 20480, 20480,
	}
	if !reflect.DeepEqual(out, ref) {
		t.Errorf("expected %v, got %v", ref, out)
	}

	// 80000 byte
	out = []int64{}
	for npools := 0; npools < 12; npools++ {
		out = append(out, marena.adaptiveNumchunks(80*1000, int64(npools)))
	}
	ref = []int64{
		64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 20480, 20480, 20480,
	}
	if !reflect.DeepEqual(out, ref) {
		t.Errorf("expected %v, got %v", ref, out)
	}

	// 800*1000 byte
	out = []int64{}
	for npools := 0; npools < 12; npools++ {
		out = append(out, marena.adaptiveNumchunks(800*1000, int64(npools)))
	}
	ref = []int64{
		64, 128, 256, 512, 1024, 2048, 4096, 8192, 10485, 10485, 10485, 10485,
	}
	if !reflect.DeepEqual(out, ref) {
		t.Errorf("expected %v, got %v", ref, out)
	}

	// 8*1000000 byte
	out = []int64{}
	for npools := 0; npools < 12; npools++ {
		out = append(out, marena.adaptiveNumchunks(8*1000000, int64(npools)))
	}
	ref = []int64{
		64, 128, 256, 512, 655, 655, 655, 655, 655, 655, 655, 655,
	}
	if !reflect.DeepEqual(out, ref) {
		t.Errorf("expected %v, got %v", ref, out)
	}

	// 80*1000000 byte
	out = []int64{}
	for npools := 0; npools < 12; npools++ {
		out = append(out, marena.adaptiveNumchunks(80*1000000, int64(npools)))
	}
	ref = []int64{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}
	if !reflect.DeepEqual(out, ref) {
		t.Errorf("expected %v, got %v", ref, out)
	}

}

func BenchmarkNewarena(b *testing.B) {
	capacity := int64(10 * 1024 * 1024)
	for i := 0; i < b.N; i++ {
		NewArena(capacity, "flist")
	}
}

func BenchmarkArenaAlloc(b *testing.B) {
	capacity := int64(10 * 1024 * 1024 * 1024)
	marena := NewArena(capacity, "flist")
	for i := 0; i < 1025; i++ {
		marena.Alloc(int64(i%1024) + 1)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		//marena.Alloc(int64(i%1024) + 1)
		marena.Alloc(96)
	}
}

func BenchmarkArenaFree(b *testing.B) {
	capacity := int64(10 * 1024 * 1024 * 1024)
	marena := NewArena(capacity, "flist")
	ptrs := make([]unsafe.Pointer, 0, b.N)
	for i := 0; i < b.N; i++ {
		//ptr := marena.Alloc(int64(i%1024) + 1)
		ptr := marena.Alloc(96)
		ptrs = append(ptrs, ptr)
	}
	b.ResetTimer()
	for _, ptr := range ptrs {
		marena.Free(ptr)
	}
}

func BenchmarkArenaInfo(b *testing.B) {
	capacity := int64(10 * 1024 * 1024)
	marena := NewArena(capacity, "flist")
	for i := 0; i < 1024; i++ {
		marena.Alloc(int64(rand.Intn(1024)))
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		marena.Info()
	}
}

func BenchmarkOSMalloc(b *testing.B) {
	for i := 0; i < b.N; i++ {
		osmalloc(96)
	}
}

func BenchmarkMalloc(b *testing.B) {
	capacity := int64(10 * 1024 * 1024 * 1024)
	marena := NewArena(capacity, "flist")

	ptrs := make([]unsafe.Pointer, 0, b.N)
	for i := 0; i < b.N; i++ {
		ptr := marena.Alloc(89)
		ptrs = append(ptrs, ptr)
	}
	for _, ptr := range ptrs {
		marena.Free(ptr)
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		marena.Alloc(89)
	}
}

func BenchmarkMallocCC2(b *testing.B) {
	capacity := int64(10 * 1024 * 1024 * 1024)
	marena := NewArena(capacity, "flist")

	ptrs := make([]unsafe.Pointer, 0, b.N)
	for i := 0; i < b.N; i++ {
		ptr := marena.Alloc(89)
		ptrs = append(ptrs, ptr)
	}
	for _, ptr := range ptrs {
		marena.Free(ptr)
	}
	b.ResetTimer()

	var wg sync.WaitGroup
	do := func(wg *sync.WaitGroup) {
		for i := 0; i < b.N/2; i++ {
			marena.Alloc(89)
		}
		wg.Done()
	}
	wg.Add(2)

	go do(&wg)
	go do(&wg)

	wg.Wait()
}

func BenchmarkMallocCC4(b *testing.B) {
	capacity := int64(10 * 1024 * 1024 * 1024)
	marena := NewArena(capacity, "flist")

	ptrs := make([]unsafe.Pointer, 0, b.N)
	for i := 0; i < b.N; i++ {
		ptr := marena.Alloc(89)
		ptrs = append(ptrs, ptr)
	}
	for _, ptr := range ptrs {
		marena.Free(ptr)
	}
	b.ResetTimer()

	var wg sync.WaitGroup
	do := func(wg *sync.WaitGroup) {
		for i := 0; i < b.N/4; i++ {
			marena.Alloc(89)
		}
		wg.Done()
	}
	wg.Add(4)

	go do(&wg)
	go do(&wg)
	go do(&wg)
	go do(&wg)
	wg.Wait()
}
