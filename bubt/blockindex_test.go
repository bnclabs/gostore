package bubt

import "testing"

func TestIndex(t *testing.T) {
	index := blkindex([]uint32{1, 2, 3, 4, 5, 6, 7, 8})
	if index.footprint() != 36 {
		t.Errorf("expected %v, got %v", 36, index.footprint())
	} else if index.nextfootprint() != 40 {
		t.Errorf("expected %v, got %v", 40, index.nextfootprint())
	} else if index.length() != 8 {
		t.Errorf("expected %v, got %v", 8, index.length())
	}
}

func BenchmarkIndexFootprint(b *testing.B) {
	index := blkindex([]uint32{1, 2, 3, 4, 5, 6, 7, 8})
	for i := 0; i < b.N; i++ {
		index.footprint()
	}
}

func BenchmarkIndexNextFprint(b *testing.B) {
	index := blkindex([]uint32{1, 2, 3, 4, 5, 6, 7, 8})
	for i := 0; i < b.N; i++ {
		index.nextfootprint()
	}
}

func BenchmarkIndexLength(b *testing.B) {
	index := blkindex([]uint32{1, 2, 3, 4, 5, 6, 7, 8})
	for i := 0; i < b.N; i++ {
		index.length()
	}
}
