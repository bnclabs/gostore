package storage

import "testing"
import "fmt"

var _ = fmt.Sprintf("dummy")

func TestNewFreebits(t *testing.T) {
	cacheline := int64(64)
	for i := 0; i < 512*512; i += 8 {
		fbits := newfreebits(cacheline, int64(i))
		x := fbits.freeblocks()
		if x != int64(i) {
			t.Errorf("expected %v, got %v", i, x)
		}
		ratio := float64(fbits.sizeof()) / float64(x*64)
		if i > 512 && ratio > float64(0.005) {
			t.Errorf("for %v expected < 0.005, got %v", i, ratio)
		}
	}
}

func TestFreebitAlloc(t *testing.T) {
	cacheline, nblocks := int64(4), int64(1024+32)
	fbits := newfreebits(cacheline, nblocks)
	allocblocks := make([]int64, 0)
	for i := int64(0); i < nblocks; i++ {
		blockbit, _ := fbits.alloc()
		if blockbit == -1 {
			t.Fatalf("unexpected -1")
		}
		allocblocks = append(allocblocks, blockbit)
	}
	if x := fbits.freeblocks(); x != 0 {
		t.Errorf("expected 0, got %v", x)
	}
	for _, block := range allocblocks {
		fbits.free(block)
	}
	if x := fbits.freeblocks(); x != nblocks {
		t.Errorf("expected %v, got %v", nblocks, x)
	}
}

func BenchmarkFbitssizeof(b *testing.B) {
	cacheline, nblocks := int64(64), int64(512*512)
	fbits := newfreebits(cacheline, nblocks)
	for i := 0; i < b.N; i++ {
		fbits.sizeof()
	}
}

func BenchmarkFbitsfreeblks(b *testing.B) {
	cacheline, nblocks := int64(64), int64(512*512)
	fbits := newfreebits(cacheline, nblocks)
	for i := 0; i < b.N; i++ {
		fbits.freeblocks()
	}
}

func BenchmarkFbitsallocfree(b *testing.B) {
	cacheline, nblocks := int64(8), int64(1024) // int64(1864368)
	fbits := newfreebits(cacheline, nblocks)
	for i := 0; i < int(nblocks-1); i++ {
		fbits.alloc()
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		block, _ := fbits.alloc()
		fbits.free(block)
	}
}
