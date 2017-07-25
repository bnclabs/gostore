package malloc

import "fmt"
import "testing"
import "unsafe"
import "reflect"
import "math/rand"

import s "github.com/prataprc/gosettings"
import "github.com/prataprc/gostore/api"

var _ = fmt.Sprintf("dummy")

func TestNewmarena(t *testing.T) {
	capacity := int64(10 * 1024 * 1024)
	marena := NewArena(capacity, Defaultsettings(96, 1024*1024))
	if x := len(marena.slabs); x != 182 {
		t.Errorf("expected %v, got %v", 182, x)
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
			"minblock":  int64(0),
			"maxblock":  int64(0x1234567812344556),
			"allocator": "flist",
		})
	}()
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("expected panic")
			}
		}()
		capacity := int64(10 * 1024 * 1024)
		NewArena(capacity, s.Settings{
			"minblock":  int64(0),
			"maxblock":  int64(359399435061660672),
			"allocator": "flist",
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
			"minblock":  int64(32),
			"maxblock":  int64(1024),
			"allocator": "flist",
		})
	}()
}

func TestArenaAlloc(t *testing.T) {
	capacity := int64(10 * 1024 * 1024)
	marena := NewArena(capacity, s.Settings{
		"minblock":  int64(96),
		"maxblock":  int64(1024),
		"allocator": "flist",
	})
	ptrs, mpools := make([]unsafe.Pointer, 1024), make([]api.MemoryPool, 1024)
	for i := 0; i < 1024; i++ {
		ptrs[i], mpools[i] = marena.Alloc(1024)
		if ptrs[i] == nil || mpools[i] == nil {
			t.Errorf("unexpected allocation failure")
		}
	}
	capacity, heap, alloc, overhead := marena.Info()
	if capacity != 10485760 {
		t.Errorf("unexpected capacity %v", capacity)
	} else if heap != 1048576 {
		t.Errorf("unexpected heap %v", heap)
	} else if alloc != 1048576 {
		t.Errorf("unexpected alloc %v", alloc)
	} else if overhead != 115320 {
		t.Errorf("unexpected overhead %v", overhead)
	}

	if slabs, uzs := marena.Utilization(); len(slabs) != 1 {
		t.Errorf("unexpected %v", len(slabs))
	} else if slabs[0] != 1024 {
		t.Errorf("unexpected %v", slabs[0])
	} else if len(uzs) != 1 {
		t.Errorf("unexpected %v", len(uzs))
	} else if uzs[0] != 100 {
		t.Errorf("unexpected %v", uzs[0])
	}

	// panic case
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("expected panic")
			}
		}()
		marena.Alloc(marena.maxblock + 1)
	}()
	marena.Release()
}

func TestArenaInfo(t *testing.T) {
	capacity := int64(10 * 1024 * 1024)
	marena := NewArena(capacity, s.Settings{
		"minblock":  int64(96),
		"maxblock":  int64(1024 * 1024),
		"allocator": "flist",
	})
	_, heap, _, overhead := marena.Info()
	if overhead != 2168 {
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
		"minblock":  int64(96),
		"maxblock":  int64(10 * 1024),
		"allocator": "flist",
	})
	ref := [6]int64{2, 1, 1, 1, 1, -1}
	if !reflect.DeepEqual(ref, marena.maxchunks) {
		t.Errorf("expected %v, got %v", ref, marena.maxchunks)
	}
	// with capacity 100M
	capacity = int64(100 * 1024 * 1024)
	marena = NewArena(capacity, s.Settings{
		"minblock":  int64(96),
		"maxblock":  int64(1024 * 1024),
		"allocator": "flist",
	})
	ref = [6]int64{204, 102, 6, 1, 1, 1}
	if !reflect.DeepEqual(ref, marena.maxchunks) {
		t.Errorf("expected %v, got %v", ref, marena.maxchunks)
	}
	// with capacity 1G
	capacity = int64(1024 * 1024 * 1024)
	marena = NewArena(capacity, s.Settings{
		"minblock":  int64(96),
		"maxblock":  int64(10 * 1024 * 1024),
		"allocator": "flist",
	})
	ref = [6]int64{1024, 1024, 65, 8, 1, 1}
	if !reflect.DeepEqual(ref, marena.maxchunks) {
		t.Errorf("expected %v, got %v", ref, marena.maxchunks)
	}
	// with capacity 1T
	capacity = int64(1024 * 1024 * 1024 * 1024)
	marena = NewArena(capacity, s.Settings{
		"minblock":  int64(96),
		"maxblock":  int64(10 * 1024 * 1024 * 1024),
		"allocator": "flist",
	})
	ref = [6]int64{1024, 1024, 1024, 1024, 1024, 65}
	if !reflect.DeepEqual(ref, marena.maxchunks) {
		t.Errorf("expected %v, got %v", ref, marena.maxchunks)
	}
}

func TestNumchunks(t *testing.T) {
	capacity := int64(1024 * 1024 * 1024 * 1024)
	marena := NewArena(capacity, s.Settings{
		"minblock":  int64(96),
		"maxblock":  int64(10 * 1024 * 1024 * 1024),
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
		"minblock":  int64(96),
		"maxblock":  int64(1024 * 1024),
		"allocator": "flist",
	}
	for i := 0; i < b.N; i++ {
		NewArena(capacity, setts)
	}
}

func BenchmarkArenaAlloc(b *testing.B) {
	capacity := int64(100 * 1024 * 1024)
	marena := NewArena(capacity, s.Settings{
		"minblock":  int64(96),
		"maxblock":  int64(1024),
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
		"minblock":  int64(96),
		"maxblock":  int64(1024),
		"allocator": "flist",
	})
	ptrs := []unsafe.Pointer{}
	pools := []api.MemoryPool{}
	for i := 0; i < b.N; i++ {
		ptr, pool := marena.Alloc(96)
		ptrs = append(ptrs, ptr)
		pools = append(pools, pool)
	}
	b.ResetTimer()
	for i, ptr := range ptrs {
		pools[i].Free(ptr)
	}
}

func BenchmarkArenaInfo(b *testing.B) {
	capacity := int64(10 * 1024 * 1024)
	marena := NewArena(capacity, s.Settings{
		"minblock":  int64(96),
		"maxblock":  int64(1024),
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
