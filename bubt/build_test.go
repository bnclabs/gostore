package bubt

import "os"
import "io"
import "fmt"
import "bytes"
import "time"
import "testing"
import "math/rand"
import "path/filepath"

import "github.com/bnclabs/gostore/llrb"
import s "github.com/bnclabs/gosettings"

func TestDestroy(t *testing.T) {
	paths := makepaths3()
	t.Log(paths)

	name, msize, zsize := "testbuild", int64(4096), int64(4096)
	bubt, err := NewBubt(name, paths, msize, zsize, 0)
	if err != nil {
		t.Fatal(err)
	}
	metadata := ""
	if err := bubt.Build(nil, []byte(metadata)); err != nil {
		t.Fatal(err)
	}
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
	paths := makepaths3()

	name, msize, zsize := "testbuild", int64(4096), int64(8192)
	bubt, err := NewBubt(name, paths, msize, zsize, -1)
	if err != nil {
		t.Fatal(err)
	}
	metadata := make([]byte, 5000)
	for i := range metadata {
		metadata[i] = byte(i % 256)
	}
	if err := bubt.Build(nil, metadata); err != nil {
		t.Fatal(err)
	}
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

func TestSnapshotGetM(t *testing.T) {
	n := 1000000
	paths := makepaths3()
	mi, _, _ := makeLLRB(n)
	defer mi.Destroy()

	t.Logf("paths %v, entries: %v", paths, n)

	rand.Seed(time.Now().UnixNano())
	name, msize := "testbuild", int64(4096)
	mmap := []bool{false, true}[rand.Intn(10000)%2]
	t.Logf("zsize: %v, vsize: 0, mmap: %v", msize, mmap)
	bubt, err := NewBubt(name, paths, msize, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	miter := mi.Scan()
	if err := bubt.Build(miter, []byte("this is metadata")); err != nil {
		t.Fatal(err)
	}
	miter(true /*fin*/)
	bubt.Close()

	t.Logf("Built Bubt ....")

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
	miter = mi.Scan()
	for key, value, seqno, deleted, err := miter(false /*fin*/); err == nil; {
		v, s, d, ok := snap.Get(key, []byte{})
		if d != deleted {
			t.Errorf("%s expected %v, got %v", key, deleted, d)
		} else if deleted == false && ok == false {
			t.Errorf("%s unexpected false", key)
		} else if s != seqno {
			t.Errorf("%s expected %v, got %v", key, seqno, s)
		} else if deleted == false && bytes.Compare(v, value) != 0 {
			t.Errorf("%s %v expected %q, got %q", key, deleted, value, v)
		}
		key, value, seqno, deleted, err = miter(false /*fin*/)
	}
	miter(true /*fin*/)
}

func TestSnapshotGetZ(t *testing.T) {
	n := 1000000
	paths := makepaths3()
	mi, _, _ := makeLLRB(n)
	defer mi.Destroy()

	t.Logf("paths %v, entries: %v", paths, n)

	rand.Seed(time.Now().UnixNano())
	name, msize := "testbuild", int64(4096)
	zsize := []int64{0, msize, msize * 2}[rand.Intn(100000)%2]
	mmap := []bool{false, true}[rand.Intn(10000)%2]
	t.Logf("zsize: %v, vsize: 0, mmap: %v", zsize, mmap)
	bubt, err := NewBubt(name, paths, msize, zsize, 0)
	if err != nil {
		t.Fatal(err)
	}
	miter := mi.Scan()
	if err := bubt.Build(miter, []byte("this is metadata")); err != nil {
		t.Fatal(err)
	}
	miter(true /*fin*/)
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
	miter = mi.Scan()
	for key, value, seqno, deleted, err := miter(false /*fin*/); err == nil; {
		v, s, d, ok := snap.Get(key, []byte{})
		if d != deleted {
			t.Errorf("%s expected %v, got %v", key, deleted, d)
		} else if deleted == false && ok == false {
			t.Errorf("%s unexpected false", key)
		} else if s != seqno {
			t.Errorf("%s expected %v, got %v", key, seqno, s)
		} else if deleted == false && bytes.Compare(v, value) != 0 {
			t.Errorf("%s expected %q, got %q", key, value, v)
		}
		key, value, seqno, deleted, err = miter(false /*fin*/)
	}
	miter(true /*fin*/)
}

func TestSnapshotGetV(t *testing.T) {
	n := 1000000
	paths := makepaths3()
	mi, _, _ := makeLLRB(n)
	defer mi.Destroy()

	t.Logf("paths %v, entries: %v", paths, n)

	rand.Seed(time.Now().UnixNano())
	name, msize := "testbuild", int64(4096)
	zsize := []int64{0, msize, msize * 2}[rand.Intn(100000)%2]
	vsize := []int64{0, zsize, zsize * 2}[rand.Intn(100000)%2]
	mmap := []bool{false, true}[rand.Intn(10000)%2]
	t.Logf("zsize: %v, vsize: %v, mmap: %v", zsize, vsize, mmap)
	bubt, err := NewBubt(name, paths, msize, zsize, vsize)
	if err != nil {
		t.Fatal(err)
	}
	miter := mi.Scan()
	if err := bubt.Build(miter, []byte("this is metadata")); err != nil {
		t.Fatal(err)
	}
	miter(true /*fin*/)
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
	miter = mi.Scan()
	for key, value, seqno, deleted, err := miter(false /*fin*/); err == nil; {
		v, s, d, ok := snap.Get(key, []byte{})
		if d != deleted {
			t.Errorf("%s expected %v, got %v", key, deleted, d)
		} else if deleted == false && ok == false {
			t.Errorf("%s unexpected false", key)
		} else if s != seqno {
			t.Errorf("%s expected %v, got %v", key, seqno, s)
		} else if deleted == false && bytes.Compare(v, value) != 0 {
			t.Errorf("%s expected %q, got %q", key, value, v)
		}
		key, value, seqno, deleted, err = miter(false /*fin*/)
	}
	miter(true /*fin*/)
}

func TestTombstonePurge(t *testing.T) {
	n := 1000000
	paths := makepaths3()
	mi, _, ndel := makeLLRB(n)
	defer mi.Destroy()

	t.Logf("paths %v, entries: %v", paths, n)

	rand.Seed(time.Now().UnixNano())
	name, msize := "testbuild", int64(4096)
	zsize := []int64{0, msize, msize * 2}[rand.Intn(100000)%2]
	vsize := []int64{0, zsize, zsize * 2}[rand.Intn(100000)%2]
	mmap := []bool{false, true}[rand.Intn(10000)%2]
	t.Logf("zsize: %v, vsize: %v, mmap: %v", zsize, vsize, mmap)
	bubt, err := NewBubt(name, paths, msize, zsize, vsize)
	if err != nil {
		t.Fatal(err)
	}
	bubt.TombstonePurge(true)
	miter := mi.Scan()
	if err := bubt.Build(miter, []byte("this is metadata")); err != nil {
		t.Fatal(err)
	}
	miter(true /*fin*/)
	bubt.Close()

	snap, err := OpenSnapshot(name, paths, mmap)
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Destroy()
	defer snap.Close()

	if x, y := snap.Count(), (mi.Count() - ndel); x != y {
		t.Errorf("expected %v, got %v", y, x)
	} else if snap.ID() != name {
		t.Errorf("expected %v, got %v", name, snap.ID())
	}
	miter = mi.Scan()
	for key, value, seqno, deleted, err := miter(false /*fin*/); err == nil; {
		if deleted == false {
			v, s, d, ok := snap.Get(key, []byte{})
			if d {
				t.Errorf("%s un expected tombstone", key)
			} else if ok == false {
				t.Errorf("%s unexpected false", key)
			} else if s != seqno {
				t.Errorf("%s expected %v, got %v", key, seqno, s)
			} else if bytes.Compare(v, value) != 0 {
				t.Errorf("%s expected %q, got %q", key, value, v)
			}
		}
		key, value, seqno, deleted, err = miter(false /*fin*/)
	}
	miter(true /*fin*/)
}

func TestSnapshotScanM1(t *testing.T) {
	n := 1000000
	paths := makepaths1()
	mi, _, _ := makeLLRB(n)
	defer mi.Destroy()

	t.Logf("paths %v, entries: %v", paths, n)

	rand.Seed(time.Now().UnixNano())
	name, msize := "testbuild", int64(4096)
	mmap := []bool{false, true}[rand.Intn(10000)%2]
	t.Logf("zsize: %v, vsize: 0 mmap: %v", msize, mmap)
	bubt, err := NewBubt(name, paths, msize, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	miter := mi.Scan()
	if err := bubt.Build(miter, []byte("this is metadata")); err != nil {
		t.Fatal(err)
	}
	miter(true /*fin*/)
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
	for key, value, seqno, deleted, err := miter(false /*fin*/); err == nil; {
		k, v, s, d, err1 := diter(false /*fin*/)
		if err1 != nil {
			t.Errorf("unexpected %v", err1)
			break
		} else if bytes.Compare(k, key) != 0 {
			t.Errorf("expected %q, got %q", key, k)
			break
		} else if d != deleted {
			t.Errorf("%s expected %v, got %v", key, deleted, d)
			break
		} else if s != seqno {
			t.Errorf("%s expected %v, got %v", key, seqno, s)
			break
		} else if deleted == false && bytes.Compare(v, value) != 0 {
			t.Errorf("%s expected %q, got %q", key, value, v)
			break
		}
		key, value, seqno, deleted, err = miter(false /*fin*/)
	}
	k, _, _, _, err := diter(false /*fin*/)
	if k != nil {
		t.Errorf("unexpected %q", k)
	} else if err != io.EOF {
		t.Errorf("unexpected %v", err)
	}
	miter(true /*fin*/)
	diter(true /*fin*/)
}

func TestSnapshotScanM2(t *testing.T) {
	n := 1000000
	paths := makepaths3()
	mi, _, _ := makeLLRB(n)
	defer mi.Destroy()

	t.Logf("paths %v, entries: %v", paths, n)

	rand.Seed(time.Now().UnixNano())
	name, msize := "testbuild", int64(4096)
	mmap := []bool{false, true}[rand.Intn(10000)%2]
	t.Logf("zsize: %v, vsize: 0 mmap: %v", msize, mmap)
	bubt, err := NewBubt(name, paths, msize, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	miter := mi.Scan()
	if err := bubt.Build(miter, []byte("this is metadata")); err != nil {
		t.Fatal(err)
	}
	miter(true /*fin*/)
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
	for key, value, seqno, deleted, err := miter(false /*fin*/); err == nil; {
		k, v, s, d, err1 := diter(false /*fin*/)
		if err1 != nil {
			t.Errorf("unexpected %v", err1)
		} else if bytes.Compare(k, key) != 0 {
			t.Errorf("expected %q, got %q", key, k)
		} else if d != deleted {
			t.Errorf("%s expected %v, got %v", key, deleted, d)
		} else if s != seqno {
			t.Errorf("%s expected %v, got %v", key, seqno, s)
		} else if deleted == false && bytes.Compare(v, value) != 0 {
			t.Errorf("%s expected %q, got %q", key, value, v)
		}
		key, value, seqno, deleted, err = miter(false /*fin*/)
	}
	k, _, _, _, err := diter(false /*fin*/)
	if k != nil {
		t.Errorf("unexpected %q", k)
	} else if err != io.EOF {
		t.Errorf("unexpected %v", err)
	}
	miter(true /*fin*/)
	diter(true /*fin*/)
}

func TestSnapshotScanM3(t *testing.T) {
	n := 1000000
	paths := makepaths3()
	mi, _, _ := makeLLRB(n)
	defer mi.Destroy()

	t.Logf("paths %v, entries: %v", paths, n)

	rand.Seed(time.Now().UnixNano())
	name, msize := "testbuild", int64(2048)
	mmap := []bool{false, true}[rand.Intn(10000)%2]
	t.Logf("zsize: %v, vsize: 0 mmap: %v", msize, mmap)
	bubt, err := NewBubt(name, paths, msize, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	miter := mi.Scan()
	if err := bubt.Build(miter, []byte("this is metadata")); err != nil {
		t.Fatal(err)
	}
	miter(true /*fin*/)
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
	for key, value, seqno, deleted, err := miter(false /*fin*/); err == nil; {
		k, v, s, d, err1 := diter(false /*fin*/)
		if err1 != nil {
			t.Errorf("unexpected %v", err1)
		} else if bytes.Compare(k, key) != 0 {
			t.Errorf("expected %q, got %q", key, k)
		} else if d != deleted {
			t.Errorf("%s expected %v, got %v", key, deleted, d)
		} else if s != seqno {
			t.Errorf("%s expected %v, got %v", key, seqno, s)
		} else if deleted == false && bytes.Compare(v, value) != 0 {
			t.Errorf("%s expected %q, got %q", key, value, v)
		}
		key, value, seqno, deleted, err = miter(false /*fin*/)
	}
	k, _, _, _, err := diter(false /*fin*/)
	if k != nil {
		t.Errorf("unexpected %q", k)
	} else if err != io.EOF {
		t.Errorf("unexpected %v", err)
	}
	miter(true /*fin*/)
	diter(true /*fin*/)
}

func TestSnapshotScanZ1(t *testing.T) {
	n := 1000000
	paths := makepaths1()
	mi, _, _ := makeLLRB(n)
	defer mi.Destroy()

	t.Logf("paths %v, entries: %v", paths, n)

	rand.Seed(time.Now().UnixNano())
	name, msize := "testbuild", int64(4096)
	zsize := []int64{0, msize, msize * 2}[rand.Intn(100000)%2]
	mmap := []bool{false, true}[rand.Intn(10000)%2]
	t.Logf("zsize: %v, vsize : 0, mmap: %v", zsize, mmap)
	bubt, err := NewBubt(name, paths, msize, zsize, 0)
	if err != nil {
		t.Fatal(err)
	}
	miter := mi.Scan()
	if err := bubt.Build(miter, []byte("this is metadata")); err != nil {
		t.Fatal(err)
	}
	miter(true /*fin*/)
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
	for key, value, seqno, deleted, err := miter(false /*fin*/); err == nil; {
		k, v, s, d, err1 := diter(false /*fin*/)
		if err1 != nil {
			t.Errorf("unexpected %v", err1)
			break
		} else if bytes.Compare(k, key) != 0 {
			t.Errorf("expected %q, got %q", key, k)
			break
		} else if d != deleted {
			t.Errorf("%s expected %v, got %v", key, deleted, d)
			break
		} else if s != seqno {
			t.Errorf("%s expected %v, got %v", key, seqno, s)
			break
		} else if deleted == false && bytes.Compare(v, value) != 0 {
			t.Errorf("%s expected %q, got %q", key, value, v)
			break
		}
		key, value, seqno, deleted, err = miter(false /*fin*/)
	}
	k, _, _, _, err := diter(false /*fin*/)
	if k != nil {
		t.Errorf("unexpected %q", k)
	} else if err != io.EOF {
		t.Errorf("unexpected %v", err)
	}
	miter(true /*fin*/)
	diter(true /*fin*/)
}

func TestSnapshotScanZ2(t *testing.T) {
	n := 1000000
	paths := makepaths3()
	mi, _, _ := makeLLRB(n)
	defer mi.Destroy()

	t.Logf("paths %v, entries: %v", paths, n)

	rand.Seed(time.Now().UnixNano())
	name, msize := "testbuild", int64(4096)
	zsize := []int64{0, msize, msize * 2}[rand.Intn(100000)%2]
	mmap := []bool{false, true}[rand.Intn(10000)%2]
	t.Logf("zsize: %v, vsize: 0, mmap: %v", zsize, mmap)
	bubt, err := NewBubt(name, paths, msize, zsize, -1)
	if err != nil {
		t.Fatal(err)
	}
	miter := mi.Scan()
	if err := bubt.Build(miter, []byte("this is metadata")); err != nil {
		t.Fatal(err)
	}
	miter(true /*fin*/)
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
	for key, value, seqno, deleted, err := miter(false /*fin*/); err == nil; {
		k, v, s, d, err1 := diter(false /*fin*/)
		if err1 != nil {
			t.Errorf("unexpected %v", err1)
		} else if bytes.Compare(k, key) != 0 {
			t.Errorf("expected %q, got %q", key, k)
		} else if d != deleted {
			t.Errorf("%s expected %v, got %v", key, deleted, d)
		} else if s != seqno {
			t.Errorf("%s expected %v, got %v", key, seqno, s)
		} else if deleted == false && bytes.Compare(v, value) != 0 {
			t.Errorf("%s expected %q, got %q", key, value, v)
		}
		key, value, seqno, deleted, err = miter(false /*fin*/)
	}
	k, _, _, _, err := diter(false /*fin*/)
	if k != nil {
		t.Errorf("unexpected %q", k)
	} else if err != io.EOF {
		t.Errorf("unexpected %v", err)
	}
	miter(true /*fin*/)
	diter(true /*fin*/)
}

func TestSnapshotScanZ3(t *testing.T) {
	n := 1000000
	paths := makepaths3()
	mi, _, _ := makeLLRB(n)
	defer mi.Destroy()

	t.Logf("paths %v, entries: %v", paths, n)

	rand.Seed(time.Now().UnixNano())
	name, msize, zsize := "testbuild", int64(2048), int64(2048)
	mmap := []bool{false, true}[rand.Intn(10000)%2]
	t.Logf("zsize: %v, vsize: 0 mmap: %v", zsize, mmap)
	bubt, err := NewBubt(name, paths, msize, zsize, 0)
	if err != nil {
		t.Fatal(err)
	}
	miter := mi.Scan()
	if err := bubt.Build(miter, []byte("this is metadata")); err != nil {
		t.Fatal(err)
	}
	miter(true /*fin*/)
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
	for key, value, seqno, deleted, err := miter(false /*fin*/); err == nil; {
		k, v, s, d, err1 := diter(false /*fin*/)
		if err1 != nil {
			t.Errorf("unexpected %v", err1)
		} else if bytes.Compare(k, key) != 0 {
			t.Errorf("expected %q, got %q", key, k)
		} else if d != deleted {
			t.Errorf("%s expected %v, got %v", key, deleted, d)
		} else if s != seqno {
			t.Errorf("%s expected %v, got %v", key, seqno, s)
		} else if deleted == false && bytes.Compare(v, value) != 0 {
			t.Errorf("%s expected %q, got %q", key, value, v)
		}
		key, value, seqno, deleted, err = miter(false /*fin*/)
	}
	k, _, _, _, err := diter(false /*fin*/)
	if k != nil {
		t.Errorf("unexpected %q", k)
	} else if err != io.EOF {
		t.Errorf("unexpected %v", err)
	}
	miter(true /*fin*/)
	diter(true /*fin*/)
}

func TestSnapshotScanV1(t *testing.T) {
	n := 1000000
	paths := makepaths1()
	mi, _, _ := makeLLRB(n)
	defer mi.Destroy()

	t.Logf("paths %v, entries: %v", paths, n)

	rand.Seed(time.Now().UnixNano())
	name, msize := "testbuild", int64(4096)
	zsize := []int64{0, msize, msize * 2}[rand.Intn(100000)%2]
	vsize := []int64{0, zsize, zsize * 2}[rand.Intn(100000)%2]
	mmap := []bool{false, true}[rand.Intn(10000)%2]
	t.Logf("zsize: %v, vsize: %v, mmap: %v", zsize, vsize, mmap)
	bubt, err := NewBubt(name, paths, msize, zsize, vsize)
	if err != nil {
		t.Fatal(err)
	}
	miter := mi.Scan()
	if err := bubt.Build(miter, []byte("this is metadata")); err != nil {
		t.Fatal(err)
	}
	miter(true /*fin*/)
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
	for key, value, seqno, deleted, err := miter(false /*fin*/); err == nil; {
		k, v, s, d, err1 := diter(false /*fin*/)
		if err1 != nil {
			t.Errorf("unexpected %v", err1)
			break
		} else if bytes.Compare(k, key) != 0 {
			t.Errorf("expected %q, got %q", key, k)
			break
		} else if d != deleted {
			t.Errorf("%s expected %v, got %v", key, deleted, d)
			break
		} else if s != seqno {
			t.Errorf("%s expected %v, got %v", key, seqno, s)
			break
		} else if deleted == false && bytes.Compare(v, value) != 0 {
			t.Errorf("%s expected %q, got %q", key, value, v)
			break
		}
		key, value, seqno, deleted, err = miter(false /*fin*/)
	}
	k, _, _, _, err := diter(false /*fin*/)
	if k != nil {
		t.Errorf("unexpected %q", k)
	} else if err != io.EOF {
		t.Errorf("unexpected %v", err)
	}
	miter(true /*fin*/)
	diter(true /*fin*/)
}

func TestSnapshotScanV2(t *testing.T) {
	n := 1000000
	paths := makepaths3()
	mi, _, _ := makeLLRB(n)
	defer mi.Destroy()

	t.Logf("paths %v, entries: %v", paths, n)

	rand.Seed(time.Now().UnixNano())
	name, msize := "testbuild", int64(4096)
	zsize := []int64{0, msize, msize * 2}[rand.Intn(100000)%2]
	vsize := []int64{0, zsize, zsize * 2}[rand.Intn(100000)%2]
	mmap := []bool{false, true}[rand.Intn(10000)%2]
	t.Logf("zsize: %v, vsize: %v mmap: %v", zsize, vsize, mmap)
	bubt, err := NewBubt(name, paths, msize, zsize, vsize)
	if err != nil {
		t.Fatal(err)
	}
	miter := mi.Scan()
	if err := bubt.Build(miter, []byte("this is metadata")); err != nil {
		t.Fatal(err)
	}
	miter(true /*fin*/)
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
	for key, value, seqno, deleted, err := miter(false /*fin*/); err == nil; {
		k, v, s, d, err1 := diter(false /*fin*/)
		if err1 != nil {
			t.Errorf("unexpected %v", err1)
		} else if bytes.Compare(k, key) != 0 {
			t.Errorf("expected %q, got %q", key, k)
		} else if d != deleted {
			t.Errorf("%s expected %v, got %v", key, deleted, d)
		} else if s != seqno {
			t.Errorf("%s expected %v, got %v", key, seqno, s)
		} else if deleted == false && bytes.Compare(v, value) != 0 {
			t.Errorf("%s expected %q, got %q", key, value, v)
		}
		key, value, seqno, deleted, err = miter(false /*fin*/)
	}
	k, _, _, _, err := diter(false /*fin*/)
	if k != nil {
		t.Errorf("unexpected %q", k)
	} else if err != io.EOF {
		t.Errorf("unexpected %v", err)
	}
	miter(true /*fin*/)
	diter(true /*fin*/)
}

func TestSnapshotScanV3(t *testing.T) {
	n := 1000000
	paths := makepaths3()
	mi, _, _ := makeLLRB(n)
	defer mi.Destroy()

	t.Logf("paths %v, entries: %v", paths, n)

	rand.Seed(time.Now().UnixNano())
	name := "testbuild"
	msize, zsize, vsize := int64(2048), int64(2048), int64(2048)
	mmap := []bool{false, true}[rand.Intn(10000)%2]
	t.Logf("zsize: %v, vsize: %v, mmap: %v", zsize, vsize, mmap)
	bubt, err := NewBubt(name, paths, msize, zsize, vsize)
	if err != nil {
		t.Fatal(err)
	}
	miter := mi.Scan()
	if err := bubt.Build(miter, []byte("this is metadata")); err != nil {
		t.Fatal(err)
	}
	miter(true /*fin*/)
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
	for key, value, seqno, deleted, err := miter(false /*fin*/); err == nil; {
		k, v, s, d, err1 := diter(false /*fin*/)
		if err1 != nil {
			t.Errorf("unexpected %v", err1)
		} else if bytes.Compare(k, key) != 0 {
			t.Errorf("expected %q, got %q", key, k)
		} else if d != deleted {
			t.Errorf("%s expected %v, got %v", key, deleted, d)
		} else if s != seqno {
			t.Errorf("%s expected %v, got %v", key, seqno, s)
		} else if deleted == false && bytes.Compare(v, value) != 0 {
			t.Errorf("%s expected %q, got %q", key, value, v)
		}
		key, value, seqno, deleted, err = miter(false /*fin*/)
	}
	k, _, _, _, err := diter(false /*fin*/)
	if k != nil {
		t.Errorf("unexpected %q", k)
	} else if err != io.EOF {
		t.Errorf("unexpected %v", err)
	}
	miter(true /*fin*/)
	diter(true /*fin*/)
}

func TestView(t *testing.T) {
	n := 1000000
	paths := makepaths3()
	mi, _, _ := makeLLRB(n)
	defer mi.Destroy()

	t.Logf("paths %v, entries: %v", paths, n)

	rand.Seed(time.Now().UnixNano())
	name, msize := "testbuild", int64(4096)
	zsize := []int64{0, msize, msize * 2}[rand.Intn(100000)%2]
	mmap := []bool{false, true}[rand.Intn(10000)%2]
	t.Logf("zsize: %v, mmap: %v", zsize, mmap)
	bubt, err := NewBubt(name, paths, msize, zsize, -1)
	if err != nil {
		t.Fatal(err)
	}
	miter := mi.Scan()
	if err := bubt.Build(miter, []byte("this is metadata")); err != nil {
		t.Fatal(err)
	}
	miter(true /*fin*/)
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
	miter = mi.Scan()
	for key, value, cas, deleted, err := miter(false /*fin*/); err == nil; {
		v, c, d, ok := view.Get(key, []byte{})
		if d != deleted {
			t.Errorf("%s expected %v, got %v", key, deleted, d)
		} else if deleted == false && ok == false {
			t.Errorf("%s unexpected false", key)
		} else if deleted == false && bytes.Compare(v, value) != 0 {
			t.Errorf("%s expected %q, got %q", key, value, v)
		} else if cas != c {
			t.Errorf("%s expected %v, got %v", key, cas, c)
		}
		key, value, cas, deleted, err = miter(false /*fin*/)
	}
	miter(true /*fin*/)
}

func TestCursorGetNext(t *testing.T) {
	n, paths := 1000, makepaths3()
	mi, _, _ := makeLLRB(n)
	defer mi.Destroy()

	rand.Seed(time.Now().UnixNano())
	name, msize := "testbuild", int64(4096)
	zsize := []int64{0, msize, msize * 2}[rand.Intn(100000)%2]
	mmap := []bool{false, true}[rand.Intn(10000)%2]
	t.Logf("zsize: %v, mmap: %v", zsize, mmap)
	bubt, err := NewBubt(name, paths, msize, zsize, 0)
	if err != nil {
		t.Fatal(err)
	}
	miter := mi.Scan()
	if err := bubt.Build(miter, []byte("this is metadata")); err != nil {
		t.Fatal(err)
	}
	miter(false /*fin*/)
	bubt.Close()

	snap, err := OpenSnapshot(name, paths, mmap)
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Destroy()
	defer snap.Close()

	id := uint64(0x12345699)
	miter = mi.Scan()
	for key, _, _, _, err := miter(false /*fin*/); err == nil; {
		dview, mview := snap.View(id), mi.View(id)
		mcur, _ := mview.OpenCursor(key)
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
			} else if d1 != d2 {
				t.Errorf("%s expected %v, got %v", key, d1, d2)
			} else if d1 == false && bytes.Compare(v1, v2) != 0 {
				t.Errorf("%s expected %q, got %q", key, v1, v2)
			}
			k3, v3, d3, err3 := mcur.GetNext()
			k4, v4, d4, err4 := dcur.GetNext()
			if err3 != err4 {
				t.Errorf("%s expected %v, got %v", key, err3, err4)
			} else if bytes.Compare(k3, k4) != 0 {
				t.Errorf("expected %q, got %q", k3, k4)
			} else if d3 != d4 {
				t.Errorf("%s expected %v, got %v", key, d3, d4)
			} else if d3 == false && bytes.Compare(v3, v4) != 0 {
				t.Errorf("%s expected %q, got %q", key, v3, v4)
			}
			if err3 != nil {
				break
			}
		}
		mview.Abort()
		dview.Abort()
		key, _, _, _, err = miter(false /*fin*/)
	}
	miter(true /*fin*/)

	// corner cases, open cursor after the end of the index.
	dview := snap.View(id)
	dcur, err1 := dview.OpenCursor([]byte("zzzzzzzzzzzzzzzzzzz"))
	if err1 != nil {
		t.Errorf("unexpected %v", err1)
	} else if key, _ := dcur.Key(); key != nil {
		t.Errorf("unexpected %q", key)
	} else if value := dcur.Value(); key != nil {
		t.Errorf("unexpected %q", value)
	} else if _, _, _, err = dcur.GetNext(); err != io.EOF {
		t.Errorf("unexpected %v", err)
	}

	// check write operations.
	do := func(fn func()) {
		defer func() {
			if recover() == nil {
				t.Errorf("expected error")
			}
		}()
		fn()
	}
	do(func() { dcur.Set(nil, nil, nil) })
	do(func() { dcur.Delete(nil, nil, false) })
	do(func() { dcur.Delcursor(false) })
}

func TestCursorYNext1(t *testing.T) {
	n, paths := 10000, makepaths3()
	mi, _, _ := makeLLRB(n)
	defer mi.Destroy()

	rand.Seed(time.Now().UnixNano())
	name, msize := "testbuild", int64(4096)
	zsize := []int64{0, msize, msize * 2}[rand.Intn(100000)%2]
	mmap := []bool{false, true}[rand.Intn(10000)%2]
	t.Logf("zsize: %v, mmap: %v", zsize, mmap)
	bubt, err := NewBubt(name, paths, msize, zsize, -1)
	if err != nil {
		t.Fatal(err)
	}
	miter := mi.Scan()
	if err := bubt.Build(miter, []byte("this is metadata")); err != nil {
		t.Fatal(err)
	}
	miter(true /*fin*/)
	bubt.Close()

	snap, err := OpenSnapshot(name, paths, mmap)
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Destroy()
	defer snap.Close()

	id := uint64(0x12345699)
	miter = mi.Scan()
	for key, _, _, _, err := miter(false /*fin*/); err == nil; {
		dview, mview := snap.View(id), mi.View(id)
		mcur, _ := mview.OpenCursor(key)
		dcur, err1 := dview.OpenCursor(key)
		if err1 != nil {
			t.Error(err1)
		}
		for {
			k1, v1, s1, d1, err3 := mcur.YNext(false /*fin*/)
			k2, v2, s2, d2, err4 := dcur.YNext(false /*fin*/)
			if err3 != err4 {
				t.Errorf("%s expected %v, got %v", key, err3, err4)
			} else if bytes.Compare(k1, k2) != 0 {
				t.Errorf("expected %q, got %q", k1, k2)
			} else if s1 != s2 {
				t.Errorf("%s expected %v, got %v", key, s1, s2)
			} else if d1 != d2 {
				t.Errorf("%s expected %v, got %v", key, d1, d2)
			} else if d1 == false && bytes.Compare(v1, v2) != 0 {
				t.Errorf("%s expected %q, got %q", key, v1, v2)
			}
			if err3 != nil {
				break
			}
		}
		mview.Abort()
		dview.Abort()
		key, _, _, _, err = miter(false /*fin*/)
	}
	miter(true /*fin*/)
}

func TestCursorYNext2(t *testing.T) {
	n, paths := 10000, makepaths3()
	mi, _, _ := makeLLRB(n)
	defer mi.Destroy()

	rand.Seed(time.Now().UnixNano())
	name, msize, zsize := "testbuild", int64(4096), int64(4096)
	mmap := []bool{false, true}[rand.Intn(10000)%2]
	t.Logf("zsize: %v, mmap: %v", zsize, mmap)
	bubt, err := NewBubt(name, paths, msize, zsize, 0)
	if err != nil {
		t.Fatal(err)
	}
	miter := mi.Scan()
	if err := bubt.Build(miter, []byte("this is metadata")); err != nil {
		t.Fatal(err)
	}
	miter(true /*fin*/)
	bubt.Close()

	snap, err := OpenSnapshot(name, paths, mmap)
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Destroy()
	defer snap.Close()

	key := []byte("key11730000000")
	id := uint64(0x12345699)
	dview, mview := snap.View(id), mi.View(id)
	mcur, _ := mview.OpenCursor(key)
	dcur, _ := dview.OpenCursor(key)
	for {
		k1, v1, s1, d1, err3 := mcur.YNext(false /*fin*/)
		k2, v2, s2, d2, err4 := dcur.YNext(false /*fin*/)
		//t.Logf("test got %s %s\n", k1, k2)
		if err3 != err4 {
			t.Errorf("%s expected %v, got %v", key, err3, err4)
			break
		} else if bytes.Compare(k1, k2) != 0 {
			t.Errorf("expected %q, got %q", k1, k2)
			break
		} else if s1 != s2 {
			t.Errorf("%s expected %v, got %v", key, s1, s2)
			break
		} else if d1 != d2 {
			t.Errorf("%s expected %v, got %v", key, d1, d2)
			break
		} else if d1 == false && bytes.Compare(v1, v2) != 0 {
			t.Errorf("%s expected %q, got %q", key, v1, v2)
			break
		}
		if err3 != nil {
			break
		}
	}
	mview.Abort()
	dview.Abort()
}

func TestOddEvenGet(t *testing.T) {
	paths, nentries := makepaths3(), 100000
	mi, _ := makeLLRBEven(nentries)
	defer mi.Destroy()

	rand.Seed(time.Now().UnixNano())
	name, msize := "testbuild", int64(4096)
	zsize := []int64{0, msize, msize * 2}[rand.Intn(100000)%2]
	mmap := []bool{false, true}[rand.Intn(10000)%2]

	t.Logf("msize: %v, zsize: %v, mmap: %v", msize, zsize, mmap)
	bubt, err := NewBubt(name, paths, msize, zsize, -1)
	if err != nil {
		t.Fatal(err)
	}
	miter := mi.Scan()
	if err := bubt.Build(miter, []byte("this is metadata")); err != nil {
		t.Fatal(err)
	}
	miter(true /*fin*/)
	bubt.Close()

	snap, err := OpenSnapshot(name, paths, mmap)
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Destroy()
	defer snap.Close()

	value := make([]byte, 0, 1024)

	// get key that falls before entire key set
	key := []byte("key00000000000000/")
	_, _, _, ok := snap.Get(key, value)
	if ok == true {
		t.Errorf("expected missing, for %s", key)
	}

loop:
	for i := 0; i < nentries*2; i++ {
		key := []byte(fmt.Sprintf("key%015d", i))
		val := []byte(fmt.Sprintf("val%015d", i))
		v, cas, deleted, ok := snap.Get(key, value)
		if (i%2) == 0 && ok == false {
			t.Errorf("expected entry for %s", key)
		} else if ok == false { // odd entry
		} else {
			if ((i*2)%10) == 0 && deleted == false {
				t.Errorf("expected deleted for %s", key)
			} else {
				continue loop
			}
			if bytes.Compare(val, v) != 0 {
				t.Errorf("for %s expected %s, got %s", key, val, v)
			}
			if refcas := uint64(i/2) + 1; refcas != cas {
				t.Errorf("for %s expected %v, got %v", key, refcas, cas)
			}
		}
	}
}

func makeLLRB(n int) (*llrb.LLRB, [][]byte, int64) {
	setts := s.Settings{"memcapacity": 1024 * 1024 * 1024}
	mi := llrb.NewLLRB("buildllrb", setts)
	ndel, keys := int64(0), [][]byte{}
	for i := 0; i < n; i++ {
		ksize := 8 + rand.Intn(256)
		fmsg := fmt.Sprintf("key%%0%vd", ksize)
		key := []byte(fmt.Sprintf(fmsg, i*2))

		vsize := ksize + rand.Intn(1024)
		fmsg = fmt.Sprintf("val%%0%vd", vsize)
		val := []byte(fmt.Sprintf(fmsg, i*2))
		mi.Set(key, val, nil)
		if i%10 == 0 {
			mi.Delete(key, nil, true /*lsm*/)
			ndel++
		}
		keys = append(keys, key)
	}
	return mi, keys, ndel
}

func makeLLRBEven(n int) (*llrb.LLRB, [][]byte) {
	setts := s.Settings{"memcapacity": 1024 * 1024 * 1024}
	mi := llrb.NewLLRB("buildllrb", setts)
	keys := [][]byte{}
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("key%015d", i*2))
		val := []byte(fmt.Sprintf("val%015d", i*2))
		if (i*2)%10 == 0 {
			mi.Delete(key, nil, true /*lsm*/)
		} else {
			mi.Set(key, val, nil)
		}
		keys = append(keys, key)
	}
	return mi, keys
}

func makepaths1() []string {
	path, paths := os.TempDir(), []string{}
	for _, base := range []string{"1"} {
		paths = append(paths, filepath.Join(path, base))
	}
	return paths
}

func makepaths3() []string {
	path, paths := os.TempDir(), []string{}
	dirs := []string{"1", "2", "3"}
	n := 1 + (rand.Intn(len(dirs)*1000) % (len(dirs) - 1))
	for _, base := range dirs[:n+1] {
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
