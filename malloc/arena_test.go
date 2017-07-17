package malloc

import "testing"
import "math/rand"
import "unsafe"
import "fmt"

import s "github.com/prataprc/gosettings"
import "github.com/prataprc/gostore/api"

var _ = fmt.Sprintf("dummy")

func TestNewmarena(t *testing.T) {
	marena := NewArena(s.Settings{
		"minblock":      int64(96),
		"maxblock":      int64(1024 * 1024 * 1024),
		"capacity":      int64(1024 * 1024 * 1024 * 1024),
		"pool.capacity": int64(1024 * 1024),
		"maxpools":      Maxpools,
		"maxchunks":     Maxchunks,
		"allocator":     "flist",
	})
	if x := len(marena.blocksizes); x != 324 {
		t.Errorf("expected %v, got %v", 118, x)
	}
	if x, y := len(marena.blocksizes), len(marena.mpools); x != y {
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
		NewArena(s.Settings{
			"minblock":      int64(0),
			"maxblock":      int64(0x1234567812344556),
			"capacity":      int64(1024 * 1024 * 1024),
			"pool.capacity": int64(1024 * 1024),
			"maxpools":      Maxpools,
			"maxchunks":     Maxchunks,
			"allocator":     "flist",
		})
	}()
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("expected panic")
			}
		}()
		NewArena(s.Settings{
			"minblock":      int64(0),
			"maxblock":      int64(359399435061660672),
			"capacity":      int64(1024 * 1024 * 1024),
			"pool.capacity": int64(1024 * 1024),
			"maxpools":      Maxpools,
			"maxchunks":     Maxchunks,
			"allocator":     "flist",
		})
	}()
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("expected panic")
			}
		}()
		NewArena(s.Settings{
			"minblock":      int64(32),
			"maxblock":      int64(1024),
			"capacity":      Maxarenasize + 1,
			"pool.capacity": int64(1024 * 1024),
			"maxpools":      Maxpools,
			"maxchunks":     Maxchunks,
			"allocator":     "flist",
		})
	}()
}

func TestArenaAlloc(t *testing.T) {
	marena := NewArena(s.Settings{
		"minblock":      int64(96),
		"maxblock":      int64(1024),
		"capacity":      int64(1024 * 1024 * 1024),
		"pool.capacity": int64(1024 * 1024),
		"maxpools":      Maxpools,
		"maxchunks":     Maxchunks,
		"allocator":     "flist",
	})
	ptrs, mpools := make([]unsafe.Pointer, 1024), make([]api.MemoryPool, 1024)
	for i := 0; i < 1024; i++ {
		ptrs[i], mpools[i] = marena.Alloc(1024)
		if ptrs[i] == nil || mpools[i] == nil {
			t.Errorf("unexpected allocation failure")
		}
	}
	if x := marena.Available(); x != 1072693248 {
		t.Errorf("expected %v, got %v", 1072693248, x)
	} else if x, y := marena.Allocated(), int64(1024*1024); x != y {
		t.Errorf("expected %v, got %v", x, y)
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

func TestArenaMemory(t *testing.T) {
	marena := NewArena(s.Settings{
		"minblock":      int64(96),
		"maxblock":      int64(1024 * 1024 * 1024),
		"capacity":      int64(1024 * 1024 * 1024 * 1024),
		"pool.capacity": int64(1024 * 1024),
		"maxpools":      Maxpools,
		"maxchunks":     Maxchunks,
		"allocator":     "flist",
	})
	if x, y := marena.Memory(); x != 4200 {
		t.Errorf("expected %v, got %v", 4200, x)
	} else if y != 0 {
		t.Errorf("expected %v, got %v", 0, y)
	}
	marena.Release()
}

func BenchmarkNewarena(b *testing.B) {
	setts := s.Settings{
		"minblock":      int64(96),
		"maxblock":      int64(1024 * 1024 * 1024),
		"capacity":      int64(1024 * 1024 * 1024 * 1024),
		"pool.capacity": int64(1024 * 1024),
		"maxpools":      Maxpools,
		"maxchunks":     Maxchunks,
		"allocator":     "flist",
	}
	for i := 0; i < b.N; i++ {
		NewArena(setts)
	}
}

func BenchmarkArenaAlloc(b *testing.B) {
	marena := NewArena(s.Settings{
		"minblock":      int64(96),
		"maxblock":      int64(1024 * 1024 * 1024),
		"capacity":      int64(1024 * 1024 * 1024 * 1024),
		"pool.capacity": int64(1024 * 1024),
		"maxpools":      Maxpools,
		"maxchunks":     Maxchunks,
		"allocator":     "flist",
	})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if ptr, mpool := marena.Alloc(96); ptr == nil || mpool == nil {
			b.Errorf("unexpected failure in allocation")
		}
	}
}

func BenchmarkArenaMemory(b *testing.B) {
	marena := NewArena(s.Settings{
		"minblock":      int64(96),
		"maxblock":      int64(1024 * 1024 * 1024),
		"capacity":      int64(3 * 1024 * 1024 * 1024),
		"pool.capacity": int64(1024 * 1024),
		"maxpools":      Maxpools,
		"maxchunks":     Maxchunks,
		"allocator":     "flist",
	})
	for i := 0; i < 1024*1024; i++ {
		marena.Alloc(int64(rand.Intn(2048)))
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		marena.Memory()
	}
}

func BenchmarkArenaAllocated(b *testing.B) {
	marena := NewArena(s.Settings{
		"minblock":      int64(96),
		"maxblock":      int64(1024 * 1024 * 1024),
		"capacity":      int64(3 * 1024 * 1024 * 1024),
		"pool.capacity": int64(1024 * 1024),
		"maxpools":      Maxpools,
		"maxchunks":     Maxchunks,
		"allocator":     "flist",
	})
	for i := 0; i < 1024*1024; i++ {
		marena.Alloc(int64(rand.Intn(2048)))
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		marena.Allocated()
	}
}
