package malloc

import "testing"
import "math/rand"
import "unsafe"
import "fmt"

import s "github.com/prataprc/gosettings"
import "github.com/prataprc/gostore/api"

var _ = fmt.Sprintf("dummy")

func TestNewmarena(t *testing.T) {
	capacity := int64(10 * 1024 * 1024)
	marena := NewArena(capacity, s.Settings{
		"minblock":  int64(96),
		"maxblock":  int64(1024 * 1024),
		"allocator": "flist",
	})
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
	} else if heap != 1245184 {
		t.Errorf("unexpected heap %v", heap)
	} else if alloc != 1048576 {
		t.Errorf("unexpected alloc %v", alloc)
	} else if overhead != 776 {
		t.Errorf("unexpected overhead %v", overhead)
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
	if overhead != 2136 {
		t.Errorf("unexpected overhead %v", overhead)
	} else if heap != 0 {
		t.Errorf("unexpected overhead %v", heap)
	}
	marena.Release()
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
		if ptr, mpool := marena.Alloc(96); ptr == nil || mpool == nil {
			b.Errorf("unexpected failure in allocation")
		}
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
