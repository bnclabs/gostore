package lsm

import "bytes"
import "testing"

import "github.com/prataprc/gostore/llrb"
import "github.com/prataprc/gostore/bubt"
import s "github.com/prataprc/gosettings"

func TestYSort(t *testing.T) {
	setts := s.Settings{
		"keycapacity": 1024 * 1024 * 1024,
		"valcapacity": 1024 * 1024 * 1024,
	}
	ref := llrb.NewLLRB("refllrb", setts)

	llrb1, keys := makeLLRB("llrb1", 100000, nil, ref, -1, -1)
	llrb2, keys := makeLLRB("llrb2", 0, keys, ref, 4, 8)
	llrb3, keys := makeLLRB("llrb3", 0, keys, ref, 4, 8)
	llrb4, _ := makeLLRB("llrb4", 0, keys, ref, 4, 8)
	defer llrb1.Destroy()
	defer llrb2.Destroy()
	defer llrb3.Destroy()
	defer llrb4.Destroy()

	t.Logf("llrb1 has %v items", llrb1.Count())
	t.Logf("llrb2 has %v items", llrb2.Count())
	t.Logf("llrb3 has %v items", llrb3.Count())
	t.Logf("llrb4 has %v items", llrb4.Count())

	paths := makepaths()

	name, msize, zsize, mmap := "bubt1", int64(4096), int64(4096), false
	bb, err := bubt.NewBubt(name, paths, msize, zsize)
	if err != nil {
		t.Fatal(err)
	}
	err = bb.Build(llrb1.Scan(), []byte("this is metadata for llrb1"))
	if err != nil {
		t.Fatal(err)
	}
	bb.Close()

	bubt1, err := bubt.OpenSnapshot(name, paths, mmap)
	if err != nil {
		t.Fatal(err)
	}

	name, msize, zsize, mmap = "bubt2", int64(4096), int64(4096)*2, true
	bb, err = bubt.NewBubt(name, paths, msize, zsize)
	if err != nil {
		t.Fatal(err)
	}
	err = bb.Build(llrb2.Scan(), []byte("this is metadata for bubt4"))
	if err != nil {
		t.Fatal(err)
	}
	bb.Close()

	bubt2, err := bubt.OpenSnapshot(name, paths, mmap)
	if err != nil {
		t.Fatal(err)
	}

	defer bubt2.Destroy()
	defer bubt1.Destroy()
	defer bubt2.Close()
	defer bubt1.Close()

	refiter := ref.Scan()
	miter := YSort(llrb3.Scan(), llrb4.Scan())
	iter := YSort(bubt1.Scan(), YSort(bubt2.Scan(), miter))
	key, value, seqno, deleted, err := refiter(false)
	for err == nil {
		k, v, s, d, e := iter(false)
		//fmt.Printf("iter %q %q %v %v %v\n", k, v, s, d, e)
		if bytes.Compare(key, k) != 0 {
			t.Errorf("expected %q, got %q", key, k)
		} else if err != e {
			t.Errorf("%q expected %v, got %v", key, err, e)
		} else if d != deleted {
			t.Errorf("%q expected %v, got %v", key, deleted, d)
		} else if s != seqno {
			t.Errorf("%q expected %v, got %v", key, seqno, s)
		} else if deleted == false && bytes.Compare(value, v) != 0 {
			t.Errorf("%q expected %q, got %q", key, value, v)
		}
		key, value, seqno, deleted, err = refiter(false)
	}
	_, _, _, _, e := iter(false)
	if e != err {
		t.Errorf("unexpected %v", e)
	}
}

func BenchmarkYSort(b *testing.B) {
	setts := s.Settings{
		"keycapacity": 1024 * 1024 * 1024,
		"valcapacity": 1024 * 1024 * 1024,
	}
	ref := llrb.NewLLRB("refllrb", setts)

	llrb1, nkeys := makeLLRB("llrb1", b.N, nil, ref, -1, -1)
	llrb2, keys := makeLLRB("llrb2", 0, nkeys, ref, 4, 8)
	llrb3, keys := makeLLRB("llrb3", 0, keys, ref, 4, 8)
	llrb4, _ := makeLLRB("llrbr", 0, keys, ref, 4, 8)
	defer llrb1.Destroy()
	defer llrb2.Destroy()
	defer llrb3.Destroy()
	defer llrb4.Destroy()

	paths := makepaths()

	name, msize, zsize, mmap := "bubt1", int64(4096), int64(4096), false
	bb, err := bubt.NewBubt(name, paths, msize, zsize)
	if err != nil {
		b.Fatal(err)
	}
	err = bb.Build(llrb1.Scan(), []byte("this is metadata for llrb1"))
	if err != nil {
		b.Fatal(err)
	}
	bb.Close()

	bubt1, err := bubt.OpenSnapshot(name, paths, mmap)
	if err != nil {
		b.Fatal(err)
	}

	name, msize, zsize, mmap = "bubt2", int64(4096), int64(4096)*2, true
	bb, err = bubt.NewBubt(name, paths, msize, zsize)
	if err != nil {
		b.Fatal(err)
	}
	err = bb.Build(llrb2.Scan(), []byte("this is metadata for bubt4"))
	if err != nil {
		b.Fatal(err)
	}
	bb.Close()

	bubt2, err := bubt.OpenSnapshot(name, paths, mmap)
	if err != nil {
		b.Fatal(err)
	}

	defer bubt2.Destroy()
	defer bubt1.Destroy()
	defer bubt2.Close()
	defer bubt1.Close()

	b.ResetTimer()
	miter := YSort(llrb3.Scan(), llrb4.Scan())
	iter := YSort(bubt1.Scan(), YSort(bubt2.Scan(), miter))
	for i := 0; i < b.N; i++ {
		if _, _, _, _, err := iter(false); err != nil {
			b.Fatal(err)
		}
	}
}
