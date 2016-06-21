package api

import "testing"

func TestBinarycmp(t *testing.T) {
	if Binarycmp([]byte("abcd"), []byte("abcd"), false) != 0 {
		t.Fatalf("unexpected return")
	} else if Binarycmp([]byte("abcd"), []byte("abce"), false) != -1 {
		t.Fatalf("unexpected return")
	} else if Binarycmp([]byte("abce"), []byte("abcd"), false) != 1 {
		t.Fatalf("unexpected return")
	} else if Binarycmp([]byte("ab"), []byte("abcd"), false) != -1 {
		t.Fatalf("unexpected return")
	} else if Binarycmp([]byte("abcd"), []byte("ab"), false) != 1 {
		t.Fatalf("unexpected return")
	} else if Binarycmp([]byte("abcd"), []byte("abcd"), true) != 0 {
		t.Fatalf("unexpected return")
	} else if Binarycmp([]byte("abcd"), []byte("abce"), true) != -1 {
		t.Fatalf("unexpected return")
	} else if Binarycmp([]byte("abce"), []byte("abcd"), true) != 1 {
		t.Fatalf("unexpected return")
	} else if Binarycmp([]byte("ab"), []byte("abcd"), true) != -1 {
		t.Fatalf("unexpected return")
	} else if Binarycmp([]byte("abcd"), []byte("ab"), true) != 0 {
		t.Fatalf("unexpected return")
	}
}

func BenchmarkBinarycmp(b *testing.B) {
	x, y := make([]byte, 256), make([]byte, 256)
	for i := 0; i < b.N; i++ {
		Binarycmp(x, y, false)
	}
}

func BenchmarkBinarycmpP(b *testing.B) {
	x, y := make([]byte, 256), make([]byte, 256)
	for i := 0; i < b.N; i++ {
		Binarycmp(x, y, true)
	}
}
