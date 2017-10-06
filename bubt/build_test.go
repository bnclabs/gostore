package bubt

import "os"
import "io"
import "fmt"
import "bytes"
import "time"
import "testing"
import "math/rand"
import "path/filepath"

import "github.com/prataprc/gostore/llrb"
import s "github.com/prataprc/gosettings"

func TestDestroy(t *testing.T) {
	paths := makepaths()
	t.Log(paths)

	name, msize, zsize := "testbuild", int64(4096), int64(4096)
	bubt, err := NewBubt(name, paths, msize, zsize)
	if err != nil {
		t.Fatal(err)
	}
	metadata := "metadata"
	bubt.Build(nil, []byte(metadata))
	bubt.Close()

	snap, err := OpenSnapshot(name, paths, false /*mmap*/)
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Destroy()
	defer snap.Close()

	err = hasfiles(name, paths, 12288, []uint64{4096, 4096, 4096})
	if err != nil {
		t.Error(err)
	}

	if snap.name != name {
		t.Errorf("unpexpected %q", snap.name)
	} else if snap.n_count != 0 {
		t.Errorf("unpexpected %v", snap.n_count)
	} else if snap.mblocksize != msize {
		t.Errorf("expected %v, got %v", msize, snap.mblocksize)
	} else if snap.zblocksize != zsize {
		t.Errorf("expected %v, got %v", zsize, snap.zblocksize)
	}

	if string(snap.metadata) != metadata {
		t.Errorf("expected %q, got %q", metadata, snap.metadata)
	}
}

func TestBuildMetadata(t *testing.T) {
	paths := makepaths()

	name, msize, zsize := "testbuild", int64(4096), int64(8192)
	bubt, err := NewBubt(name, paths, msize, zsize)
	if err != nil {
		t.Fatal(err)
	}
	metadata := make([]byte, 5000)
	for i := range metadata {
		metadata[i] = byte(i % 256)
	}
	bubt.Build(nil, metadata)
	bubt.Close()

	snap, err := OpenSnapshot(name, paths, false /*mmap*/)
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Destroy()
	defer snap.Close()

	err = hasfiles(name, paths, 16384, []uint64{4096, 4096, 4096})
	if err != nil {
		t.Error(err)
	}

	if snap.name != name {
		t.Errorf("unpexpected %q", snap.name)
	} else if snap.n_count != 0 {
		t.Errorf("unpexpected %v", snap.n_count)
	} else if snap.mblocksize != msize {
		t.Errorf("expected %v, got %v", msize, snap.mblocksize)
	} else if snap.zblocksize != zsize {
		t.Errorf("expected %v, got %v", zsize, snap.zblocksize)
	}

	if string(snap.metadata) != string(metadata) {
		t.Errorf("expected %q, got %q", string(metadata), string(snap.metadata))
	}
}

func TestSnapshotGet(t *testing.T) {
	paths := makepaths()
	mi, _ := makeLLRB(10000000)
	defer mi.Destroy()

	rand.Seed(time.Now().UnixNano())
	name, msize := "testbuild", int64(4096)
	zsize := []int64{msize, msize * 2}[rand.Intn(100000)%2]
	mmap := []bool{false, true}[rand.Intn(10000)%2]
	t.Logf("zsize: %v, mmap: %v", zsize, mmap)
	bubt, err := NewBubt(name, paths, msize, zsize)
	if err != nil {
		t.Fatal(err)
	}
	bubt.Build(mi.Scan(), []byte("this is metadata"))
	bubt.Close()

	snap, err := OpenSnapshot(name, paths, mmap)
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Destroy()
	defer snap.Close()

	//value, seqno, deleted, ok := snap.Get([]byte("key9998"), []byte{})
	//t.Logf("%s %v %v %v", value, seqno, deleted, ok)

	if snap.Count() != mi.Count() {
		t.Errorf("expected %v, got %v", mi.Count(), snap.Count())
	} else if snap.ID() != name {
		t.Errorf("expected %v, got %v", name, snap.ID())
	}
	miter := mi.Scan()
	for key, value, seqno, deleted, err := miter(); err == nil; {
		v, s, d, ok := snap.Get(key, []byte{})
		if d != deleted {
			t.Errorf("%s expected %v, got %v", key, deleted, d)
		} else if deleted == false && ok == false {
			t.Errorf("%s unexpected false", key)
		} else if s != seqno {
			t.Errorf("%s expected %v, got %v", key, seqno, s)
		} else if bytes.Compare(v, value) != 0 {
			t.Errorf("%s expected %q, got %q", key, value, v)
		}
		key, value, seqno, deleted, err = miter()
	}
}

func TestSnapshotScan(t *testing.T) {
	paths := makepaths()
	mi, _ := makeLLRB(10000000)
	defer mi.Destroy()

	rand.Seed(time.Now().UnixNano())
	name, msize := "testbuild", int64(4096)
	zsize := []int64{msize, msize * 2}[rand.Intn(100000)%2]
	mmap := []bool{false, true}[rand.Intn(10000)%2]
	t.Logf("zsize: %v, mmap: %v", zsize, mmap)
	bubt, err := NewBubt(name, paths, msize, zsize)
	if err != nil {
		t.Fatal(err)
	}
	bubt.Build(mi.Scan(), []byte("this is metadata"))
	bubt.Close()

	snap, err := OpenSnapshot(name, paths, mmap)
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Destroy()
	defer snap.Close()

	if snap.Count() != mi.Count() {
		t.Errorf("expected %v, got %v", mi.Count(), snap.Count())
	}
	miter, diter := mi.Scan(), snap.Scan()
	for key, value, seqno, deleted, err := miter(); err == nil; {
		k, v, s, d, err1 := diter()
		if err1 != nil {
			t.Errorf("unexpected %v", err1)
		} else if bytes.Compare(k, key) != 0 {
			t.Errorf("expected %q, got %q", key, k)
		} else if bytes.Compare(v, value) != 0 {
			t.Errorf("%s expected %q, got %q", key, value, v)
		} else if d != deleted {
			t.Errorf("%s expected %v, got %v", key, deleted, d)
		} else if s != seqno {
			t.Errorf("%s expected %v, got %v", key, seqno, s)
		}
		key, value, seqno, deleted, err = miter()
	}
	k, _, _, _, err := diter()
	if k != nil {
		t.Errorf("unexpected %q", k)
	} else if err != io.EOF {
		t.Errorf("unexpected %v", err)
	}
}

func TestView(t *testing.T) {
	paths := makepaths()
	mi, _ := makeLLRB(1000000)
	defer mi.Destroy()

	rand.Seed(time.Now().UnixNano())
	name, msize := "testbuild", int64(4096)
	zsize := []int64{msize, msize * 2}[rand.Intn(100000)%2]
	mmap := []bool{false, true}[rand.Intn(10000)%2]
	t.Logf("zsize: %v, mmap: %v", zsize, mmap)
	bubt, err := NewBubt(name, paths, msize, zsize)
	if err != nil {
		t.Fatal(err)
	}
	bubt.Build(mi.Scan(), []byte("this is metadata"))
	bubt.Close()

	snap, err := OpenSnapshot(name, paths, mmap)
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Destroy()
	defer snap.Close()

	id := uint64(0x12345699)
	view := snap.View(id)
	if view.ID() != id {
		t.Errorf("expected %v, got %v", id, view.ID())
	}
	miter := mi.Scan()
	for key, value, _, deleted, err := miter(); err == nil; {
		v, d, ok := view.Get(key, []byte{})
		if d != deleted {
			t.Errorf("%s expected %v, got %v", key, deleted, d)
		} else if deleted == false && ok == false {
			t.Errorf("%s unexpected false", key)
		} else if bytes.Compare(v, value) != 0 {
			t.Errorf("%s expected %q, got %q", key, value, v)
		}
		key, value, _, deleted, err = miter()
	}
}

func TestCursorGetNext(t *testing.T) {
	paths := makepaths()
	mi, _ := makeLLRB(10000)
	defer mi.Destroy()

	rand.Seed(time.Now().UnixNano())
	name, msize := "testbuild", int64(4096)
	zsize := []int64{msize, msize * 2}[rand.Intn(100000)%2]
	mmap := []bool{false, true}[rand.Intn(10000)%2]
	t.Logf("zsize: %v, mmap: %v", zsize, mmap)
	bubt, err := NewBubt(name, paths, msize, zsize)
	if err != nil {
		t.Fatal(err)
	}
	bubt.Build(mi.Scan(), []byte("this is metadata"))
	bubt.Close()

	snap, err := OpenSnapshot(name, paths, mmap)
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Destroy()
	defer snap.Close()

	id := uint64(0x12345699)
	miter := mi.Scan()
	for key, _, _, _, err := miter(); err == nil; {
		dview, mview := snap.View(id), mi.View(id)
		mcur := mview.OpenCursor(key)
		dcur, err1 := dview.OpenCursor(key)
		if err1 != nil {
			t.Error(err1)
		}
		for {
			k1, d1 := mcur.Key()
			v1 := mcur.Value()
			k2, d2 := dcur.Key()
			v2 := dcur.Value()
			if bytes.Compare(k1, k2) != 0 {
				t.Errorf("expected %q, got %q", k1, k2)
			} else if bytes.Compare(v1, v2) != 0 {
				t.Errorf("%s expected %q, got %q", key, v1, v2)
			} else if d1 != d2 {
				t.Errorf("%s expected %v, got %v", key, d1, d2)
			}
			k3, v3, d3, err3 := mcur.GetNext()
			k4, v4, d4, err4 := dcur.GetNext()
			if err3 != err4 {
				t.Errorf("%s expected %v, got %v", key, err3, err4)
			} else if bytes.Compare(k3, k4) != 0 {
				t.Errorf("expected %q, got %q", k3, k4)
			} else if bytes.Compare(v3, v4) != 0 {
				t.Errorf("%s expected %q, got %q", key, v3, v4)
			} else if d3 != d4 {
				t.Errorf("%s expected %v, got %v", key, d3, d4)
			}
			if err3 != nil {
				break
			}
		}
		mview.Abort()
		dview.Abort()
		key, _, _, _, err = miter()
	}
}

func TestCursorYNext(t *testing.T) {
	paths := makepaths()
	mi, _ := makeLLRB(10000)
	defer mi.Destroy()

	rand.Seed(time.Now().UnixNano())
	name, msize := "testbuild", int64(4096)
	zsize := []int64{msize, msize * 2}[rand.Intn(100000)%2]
	mmap := []bool{false, true}[rand.Intn(10000)%2]
	t.Logf("zsize: %v, mmap: %v", zsize, mmap)
	bubt, err := NewBubt(name, paths, msize, zsize)
	if err != nil {
		t.Fatal(err)
	}
	bubt.Build(mi.Scan(), []byte("this is metadata"))
	bubt.Close()

	snap, err := OpenSnapshot(name, paths, mmap)
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Destroy()
	defer snap.Close()

	id := uint64(0x12345699)
	miter := mi.Scan()
	for key, _, _, _, err := miter(); err == nil; {
		dview, mview := snap.View(id), mi.View(id)
		mcur := mview.OpenCursor(key)
		dcur, err1 := dview.OpenCursor(key)
		if err1 != nil {
			t.Error(err1)
		}
		for {
			k1, v1, s1, d1, err3 := mcur.YNext()
			k2, v2, s2, d2, err4 := dcur.YNext()
			if err3 != err4 {
				t.Errorf("%s expected %v, got %v", key, err3, err4)
			} else if bytes.Compare(k1, k2) != 0 {
				t.Errorf("expected %q, got %q", k1, k2)
			} else if bytes.Compare(v1, v2) != 0 {
				t.Errorf("%s expected %q, got %q", key, v1, v2)
			} else if s1 != s2 {
				t.Errorf("%s expected %v, got %v", key, s1, s2)
			} else if d1 != d2 {
				t.Errorf("%s expected %v, got %v", key, d1, d2)
			}
			if err3 != nil {
				break
			}
		}
		mview.Abort()
		dview.Abort()
		key, _, _, _, err = miter()
	}
}

func makeLLRB(n int) (*llrb.LLRB, [][]byte) {
	setts := s.Settings{
		"keycapacity": 1024 * 1024 * 1024, "valcapacity": 1024 * 1024 * 1024,
	}
	mi := llrb.NewLLRB("buildllrb", setts)
	k, v := []byte("key000000000000"), []byte("val00000000000000")
	keys := [][]byte{}
	for i := 0; i < n; i++ {
		x := fmt.Sprintf("%d", i)
		key, val := append(k[:3], x...), append(v[:3], x...)
		mi.Set(key, val, nil)
		if i%10 == 0 {
			mi.Delete(key, nil, true /*lsm*/)
		}
		keys = append(keys, key)
	}
	return mi, keys
}

func makepaths() []string {
	path, paths := os.TempDir(), []string{}
	for _, base := range []string{"1", "2", "3"} {
		paths = append(paths, filepath.Join(path, base))
	}
	return paths
}

func hasfiles(
	name string, paths []string, msize uint64, zsizes []uint64) error {

	fi, err := os.Stat(filepath.Join(paths[0], name, "bubt-mindex.data"))
	if err != nil {
		return err
	} else if fi.Size() != int64(msize) {
		return fmt.Errorf("msize expected %v, got %v", msize, fi.Size())
	}

	for i, path := range paths {
		base := fmt.Sprintf("bubt-zindex-%v.data", i+1)
		fi, err := os.Stat(filepath.Join(path, name, base))
		if err != nil {
			return err
		} else if zsize := zsizes[i]; fi.Size() != int64(zsize) {
			return fmt.Errorf("msize expected %v, got %v", zsize, fi.Size())
		}
	}
	return nil
}
