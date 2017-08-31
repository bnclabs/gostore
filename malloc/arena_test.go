package malloc

import "fmt"
import "testing"
import "unsafe"
import "reflect"
import "math/rand"

import s "github.com/prataprc/gosettings"

var _ = fmt.Sprintf("dummy")

func TestNewmarena(t *testing.T) {
	capacity := int64(10 * 1024 * 1024)
	marena := NewArena(capacity, Defaultsettings())
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
		NewArena(capacity, s.Settings{
			"allocator": "fbit",
		})
	}()
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("expected panic")
			}
		}()
		capacity := Maxarenasize + 1
		NewArena(capacity, s.Settings{
			"allocator": "flist",
		})
	}()
}

func TestArenaAlloc(t *testing.T) {
	capacity := int64(10 * 1024 * 1024)
	marena := NewArena(capacity, s.Settings{
		"allocator": "flist",
	})
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
	} else if heap != 1100944 {
		t.Errorf("unexpected heap %v", heap)
	} else if alloc != 1097728 {
		t.Errorf("unexpected alloc %v", alloc)
	} else if overhead != 31440 {
		t.Errorf("unexpected overhead %v", overhead)
	}

	if slabs, uzs := marena.Utilization(); len(slabs) != 1 {
		t.Errorf("unexpected %v", len(slabs))
	} else if slabs[0] != 1072 {
		t.Errorf("unexpected %v", slabs[0])
	} else if len(uzs) != 1 {
		t.Errorf("unexpected %v", len(uzs))
	} else if uzs[0] < 99 {
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
	marena := NewArena(capacity, s.Settings{
		"allocator": "flist",
	})
	_, heap, _, overhead := marena.Info()
	if overhead != 12064 {
		t.Errorf("unexpected overhead %v", overhead)
	} else if heap != 0 {
		t.Errorf("unexpected overhead %v", heap)
	}
	marena.Release()
}

func TestArenaMaxchunks(t *testing.T) {
	// with capacity 1M
	capacity := int64(1024 * 1024)
	marena := NewArena(capacity, s.Settings{
		"allocator": "flist",
	})
	ref := [6]int64{20, 10, 1, 1, 1, -1}
	if !reflect.DeepEqual(ref, marena.maxchunks) {
		t.Errorf("expected %v, got %v", ref, marena.maxchunks)
	}
	// with capacity 100M
	capacity = int64(100 * 1024 * 1024)
	marena = NewArena(capacity, s.Settings{
		"allocator": "flist",
	})
	ref = [6]int64{1024, 1024, 64, 8, 1, 1}
	if !reflect.DeepEqual(ref, marena.maxchunks) {
		t.Errorf("expected %v, got %v", ref, marena.maxchunks)
	}
	// with capacity 1T
	capacity = int64(1024 * 1024 * 1024 * 1024)
	marena = NewArena(capacity, s.Settings{
		"allocator": "flist",
	})
	ref = [6]int64{1024, 1024, 1024, 1024, 1024, 655}
	if !reflect.DeepEqual(ref, marena.maxchunks) {
		t.Errorf("expected %v, got %v", ref, marena.maxchunks)
	}
}

func TestNumchunks(t *testing.T) {
	capacity := int64(1024 * 1024 * 1024 * 1024)
	marena := NewArena(capacity, s.Settings{
		"allocator": "flist",
	})
	// 100 byte
	out := []int64{}
	for npools := 0; npools < 12; npools++ {
		out = append(out, marena.adaptiveNumchunks(100, int64(npools)))
	}
	ref := []int64{1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 1024}
	if !reflect.DeepEqual(out, ref) {
		t.Errorf("expected %v, got %v", ref, out)
	}

	// 800 byte
	out = []int64{}
	for npools := 0; npools < 12; npools++ {
		out = append(out, marena.adaptiveNumchunks(800, int64(npools)))
	}
	ref = []int64{1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 1024}
	if !reflect.DeepEqual(out, ref) {
		t.Errorf("expected %v, got %v", ref, out)
	}

	// 8000 byte
	out = []int64{}
	for npools := 0; npools < 12; npools++ {
		out = append(out, marena.adaptiveNumchunks(8000, int64(npools)))
	}
	ref = []int64{1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 1024}
	if !reflect.DeepEqual(out, ref) {
		t.Errorf("expected %v, got %v", ref, out)
	}

	// 80000 byte
	out = []int64{}
	for npools := 0; npools < 12; npools++ {
		out = append(out, marena.adaptiveNumchunks(80*1000, int64(npools)))
	}
	ref = []int64{1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 1024}
	if !reflect.DeepEqual(out, ref) {
		t.Errorf("expected %v, got %v", ref, out)
	}

	// 800*1000 byte
	out = []int64{}
	for npools := 0; npools < 12; npools++ {
		out = append(out, marena.adaptiveNumchunks(800*1000, int64(npools)))
	}
	ref = []int64{1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 1024}
	if !reflect.DeepEqual(out, ref) {
		t.Errorf("expected %v, got %v", ref, out)
	}

	// 8*1000000 byte
	out = []int64{}
	for npools := 0; npools < 12; npools++ {
		out = append(out, marena.adaptiveNumchunks(8*1000000, int64(npools)))
	}
	ref = []int64{1, 2, 4, 8, 16, 32, 64, 65, 65, 65, 65, 65}
	ref = []int64{1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 655, 655}
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
	setts := s.Settings{
		"allocator": "flist",
	}
	for i := 0; i < b.N; i++ {
		NewArena(capacity, setts)
	}
}

func BenchmarkArenaAlloc(b *testing.B) {
	capacity := int64(100 * 1024 * 1024)
	marena := NewArena(capacity, s.Settings{
		"allocator": "flist",
	})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		marena.Alloc(96)
	}
}

func BenchmarkArenaFree(b *testing.B) {
	capacity := int64(100 * 1024 * 1024)
	marena := NewArena(capacity, s.Settings{
		"allocator": "flist",
	})
	ptrs := []unsafe.Pointer{}
	for i := 0; i < b.N; i++ {
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
	marena := NewArena(capacity, s.Settings{
		"allocator": "flist",
	})
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
