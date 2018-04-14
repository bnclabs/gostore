package lsm

import "fmt"
import "bytes"
import "testing"

import "github.com/bnclabs/gostore/llrb"
import "github.com/bnclabs/gostore/bubt"
import s "github.com/bnclabs/gosettings"

var _ = fmt.Sprintf("")

func TestYSortEntriesM1(t *testing.T) {
	setts := s.Settings{"memcapacity": 1024 * 1024 * 1024}
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
	bb, err := bubt.NewBubt(name, paths, msize, zsize, 0)
	if err != nil {
		t.Fatal(err)
	}
	itere := llrb1.ScanEntries()
	err = bb.Build(itere, []byte("this is metadata for llrb1"))
	if err != nil {
		t.Fatal(err)
	}
	bb.Close()
	itere(true /*fin*/)

	bubt1, err := bubt.OpenSnapshot(name, paths, mmap)
	if err != nil {
		t.Fatal(err)
	}

	name, msize, zsize, mmap = "bubt2", int64(4096), int64(4096)*2, true
	bb, err = bubt.NewBubt(name, paths, msize, zsize, 0)
	if err != nil {
		t.Fatal(err)
	}
	itere = llrb2.ScanEntries()
	err = bb.Build(itere, []byte("this is metadata for bubt4"))
	if err != nil {
		t.Fatal(err)
	}
	bb.Close()
	itere(true /*fin*/)

	bubt2, err := bubt.OpenSnapshot(name, paths, mmap)
	if err != nil {
		t.Fatal(err)
	}

	defer bubt2.Destroy()
	defer bubt1.Destroy()
	defer bubt2.Close()
	defer bubt1.Close()

	refiter := ref.Scan()
	miter := YSortEntries(llrb3.ScanEntries(), llrb4.ScanEntries())
	iterentries := YSortEntries(
		bubt1.ScanEntries(), YSortEntries(bubt2.ScanEntries(), miter),
	)
	key, value, seqno, deleted, err := refiter(false)
	for err == nil {
		entry := iterentries(false)
		k, s, d, e := entry.Key()
		v := entry.Value()
		//fmt.Printf("iterentries %q %q %v %v %v\n", k, v, s, d, e)
		//fmt.Printf("ref %q %q %v %v %v\n", key, value, seqno, deleted, err)
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
	entry := iterentries(false)
	if _, _, _, e := entry.Key(); e != err {
		t.Errorf("unexpected %v", e)
	}
	refiter(true /*fin*/)
	iterentries(true /*fin*/)
}

func TestYSortEntriesM2(t *testing.T) {
	setts := s.Settings{"memcapacity": 1024 * 1024 * 1024}
	ref := llrb.NewLLRB("refllrb", setts)

	llrb1, keys := makeLLRB("llrb1", 100000, nil, ref, -1, -1)
	llrb3, keys := makeLLRB("llrb3", 0, keys, ref, 4, 8)
	defer llrb1.Destroy()
	defer llrb3.Destroy()

	t.Logf("llrb1 has %v items", llrb1.Count())
	t.Logf("llrb3 has %v items", llrb3.Count())

	paths := makepaths()

	name, msize, zsize, mmap := "bubt1", int64(4096), int64(4096), false
	bb, err := bubt.NewBubt(name, paths, msize, zsize, 0)
	if err != nil {
		t.Fatal(err)
	}
	itere := llrb1.ScanEntries()
	err = bb.Build(itere, []byte("this is metadata for llrb1"))
	if err != nil {
		t.Fatal(err)
	}
	bb.Close()
	itere(true /*fin*/)

	bubt1, err := bubt.OpenSnapshot(name, paths, mmap)
	if err != nil {
		t.Fatal(err)
	}

	defer bubt1.Destroy()
	defer bubt1.Close()

	refiter := ref.Scan()
	miter := YSortEntries(llrb3.ScanEntries(), nil)
	iterentries := YSortEntries(bubt1.ScanEntries(), YSortEntries(nil, miter))
	key, value, seqno, deleted, err := refiter(false)
	for err == nil {
		entry := iterentries(false)
		k, s, d, e := entry.Key()
		v := entry.Value()
		//fmt.Printf("iterentries %q %q %v %v %v\n", k, v, s, d, e)
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
	entry := iterentries(false)
	if _, _, _, e := entry.Key(); e != err {
		t.Errorf("unexpected %v", e)
	}
	iterentries(true /*fin*/)
	refiter(true /*fin*/)
}

func TestYSortEntriesV1(t *testing.T) {
	setts := s.Settings{"memcapacity": 1024 * 1024 * 1024}
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

	name, msize, vsize, mmap := "bubt1", int64(4096), int64(4096*2), false
	bb, err := bubt.NewBubt(name, paths, msize, msize, vsize)
	if err != nil {
		t.Fatal(err)
	}
	itere := llrb1.ScanEntries()
	err = bb.Build(itere, []byte("this is metadata for llrb1"))
	if err != nil {
		t.Fatal(err)
	}
	bb.Close()
	itere(true /*fin*/)

	bubt1, err := bubt.OpenSnapshot(name, paths, mmap)
	if err != nil {
		t.Fatal(err)
	}

	name, mmap = "bubt2", true
	msize, zsize, vsize := int64(4096), int64(4096*2), int64(4096)*4
	bb, err = bubt.NewBubt(name, paths, msize, zsize, vsize)
	if err != nil {
		t.Fatal(err)
	}
	itere = llrb2.ScanEntries()
	err = bb.Build(itere, []byte("this is metadata for bubt4"))
	if err != nil {
		t.Fatal(err)
	}
	bb.Close()
	itere(true /*fin*/)

	bubt2, err := bubt.OpenSnapshot(name, paths, mmap)
	if err != nil {
		t.Fatal(err)
	}

	defer bubt2.Destroy()
	defer bubt1.Destroy()
	defer bubt2.Close()
	defer bubt1.Close()

	refiter := ref.Scan()
	miter := YSortEntries(llrb3.ScanEntries(), llrb4.ScanEntries())
	iterentries := YSortEntries(
		bubt1.ScanEntries(), YSortEntries(bubt2.ScanEntries(), miter),
	)
	key, value, seqno, deleted, err := refiter(false)
	for err == nil {
		entry := iterentries(false)
		k, s, d, e := entry.Key()
		v := entry.Value()
		//fmt.Printf("iterentries %q %q %v %v %v\n", k, v, s, d, e)
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
	entry := iterentries(false)
	if _, _, _, e := entry.Key(); e != err {
		t.Errorf("unexpected %v", e)
	}
	refiter(true /*fin*/)
	iterentries(true /*fin*/)
}

func TestYSortEntriesV2(t *testing.T) {
	setts := s.Settings{"memcapacity": 1024 * 1024 * 1024}
	ref := llrb.NewLLRB("refllrb", setts)

	llrb1, keys := makeLLRB("llrb1", 100000, nil, ref, -1, -1)
	llrb3, keys := makeLLRB("llrb3", 0, keys, ref, 4, 8)
	defer llrb1.Destroy()
	defer llrb3.Destroy()

	t.Logf("llrb1 has %v items", llrb1.Count())
	t.Logf("llrb3 has %v items", llrb3.Count())

	paths := makepaths()

	name, msize, vsize, mmap := "bubt1", int64(4096), int64(4096), false
	bb, err := bubt.NewBubt(name, paths, msize, msize, vsize)
	if err != nil {
		t.Fatal(err)
	}
	itere := llrb1.ScanEntries()
	err = bb.Build(itere, []byte("this is metadata for llrb1"))
	if err != nil {
		t.Fatal(err)
	}
	bb.Close()
	itere(true /*fin*/)

	bubt1, err := bubt.OpenSnapshot(name, paths, mmap)
	if err != nil {
		t.Fatal(err)
	}

	defer bubt1.Destroy()
	defer bubt1.Close()

	refiter := ref.Scan()
	miter := YSortEntries(llrb3.ScanEntries(), nil)
	iterentries := YSortEntries(bubt1.ScanEntries(), YSortEntries(nil, miter))
	key, value, seqno, deleted, err := refiter(false)
	for err == nil {
		entry := iterentries(false)
		k, s, d, e := entry.Key()
		v := entry.Value()
		//fmt.Printf("iterentries %q %q %v %v %v\n", k, v, s, d, e)
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
	entry := iterentries(false)
	if _, _, _, e := entry.Key(); e != err {
		t.Errorf("unexpected %v", e)
	}
	iterentries(true /*fin*/)
	refiter(true /*fin*/)
}

func BenchmarkYSortEntriesM(b *testing.B) {
	setts := s.Settings{"memcapacity": 1024 * 1024 * 1024}
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
	bb, err := bubt.NewBubt(name, paths, msize, zsize, 0)
	if err != nil {
		b.Fatal(err)
	}
	itere := llrb1.ScanEntries()
	err = bb.Build(itere, []byte("this is metadata for llrb1"))
	if err != nil {
		b.Fatal(err)
	}
	bb.Close()
	itere(true /*fin*/)

	bubt1, err := bubt.OpenSnapshot(name, paths, mmap)
	if err != nil {
		b.Fatal(err)
	}

	name, msize, zsize, mmap = "bubt2", int64(4096), int64(4096)*2, true
	bb, err = bubt.NewBubt(name, paths, msize, zsize, 0)
	if err != nil {
		b.Fatal(err)
	}
	itere = llrb2.ScanEntries()
	err = bb.Build(itere, []byte("this is metadata for bubt4"))
	if err != nil {
		b.Fatal(err)
	}
	bb.Close()
	itere(true /*fin*/)

	bubt2, err := bubt.OpenSnapshot(name, paths, mmap)
	if err != nil {
		b.Fatal(err)
	}

	defer bubt2.Destroy()
	defer bubt1.Destroy()
	defer bubt2.Close()
	defer bubt1.Close()

	b.ResetTimer()
	miter := YSortEntries(llrb3.ScanEntries(), llrb4.ScanEntries())
	iterentries := YSortEntries(
		bubt1.ScanEntries(), YSortEntries(bubt2.ScanEntries(), miter),
	)
	for i := 0; i < b.N; i++ {
		entry := iterentries(false)
		if _, _, _, err := entry.Key(); err != nil {
			b.Fatal(err)
		}
	}
	iterentries(true /*fin*/)
}

func BenchmarkYSortEntriesV(b *testing.B) {
	setts := s.Settings{"memcapacity": 1024 * 1024 * 1024}
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

	name, msize, vsize, mmap := "bubt1", int64(4096), int64(4096*2), false
	bb, err := bubt.NewBubt(name, paths, msize, msize, vsize)
	if err != nil {
		b.Fatal(err)
	}
	itere := llrb1.ScanEntries()
	err = bb.Build(itere, []byte("this is metadata for llrb1"))
	if err != nil {
		b.Fatal(err)
	}
	bb.Close()
	itere(true /*fin*/)

	bubt1, err := bubt.OpenSnapshot(name, paths, mmap)
	if err != nil {
		b.Fatal(err)
	}

	name, mmap = "bubt2", true
	msize, zsize, vsize := int64(4096), int64(4096)*2, int64(4096)*4
	bb, err = bubt.NewBubt(name, paths, msize, zsize, vsize)
	if err != nil {
		b.Fatal(err)
	}
	itere = llrb2.ScanEntries()
	err = bb.Build(itere, []byte("this is metadata for bubt4"))
	if err != nil {
		b.Fatal(err)
	}
	bb.Close()
	itere(true /*fin*/)

	bubt2, err := bubt.OpenSnapshot(name, paths, mmap)
	if err != nil {
		b.Fatal(err)
	}

	defer bubt2.Destroy()
	defer bubt1.Destroy()
	defer bubt2.Close()
	defer bubt1.Close()

	b.ResetTimer()
	miter := YSortEntries(llrb3.ScanEntries(), llrb4.ScanEntries())
	iterentries := YSortEntries(
		bubt1.ScanEntries(), YSortEntries(bubt2.ScanEntries(), miter),
	)
	for i := 0; i < b.N; i++ {
		entry := iterentries(false)
		if _, _, _, err := entry.Key(); err != nil {
			b.Fatal(err)
		}
	}
	iterentries(true /*fin*/)
}
