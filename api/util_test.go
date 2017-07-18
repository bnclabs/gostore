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

func TestFixbuffer(t *testing.T) {
	if ln := len(Fixbuffer(nil, 10)); ln != 10 {
		t.Errorf("expected %v, got %v", 10, ln)
	} else if ln = len(Fixbuffer(nil, 0)); ln != 0 {
		t.Errorf("expected %v, got %v", 0, ln)
	} else if ln = len(Fixbuffer([]byte{10, 20}, 0)); ln != 0 {
		t.Errorf("expected %v, got %v", 0, ln)
	}
}

func TestBytes2str(t *testing.T) {
	if Bytes2str(nil) != "" {
		t.Errorf("unexpected %v", Bytes2str(nil))
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
