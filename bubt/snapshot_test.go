package bubt

import "time"
import "testing"
import "math/rand"

func TestValidate(t *testing.T) {
	n := 1000000
	paths := makepaths3()
	mi, _ := makeLLRB(n)
	defer mi.Destroy()

	t.Logf("paths %v, entries: %v", paths, n)

	rand.Seed(time.Now().UnixNano())
	name, msize := "testbuild", int64(4096)
	zsize := []int64{0, msize, msize * 2}[rand.Intn(100000)%2]
	vsize := []int64{0, zsize, zsize * 2}[rand.Intn(100000)%2]
	mmap := []bool{false, true}[rand.Intn(10000)%2]
	t.Logf("zsize: %v, vsize: 0, mmap: %v", zsize, mmap)
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

	snap.Validate()
}

func TestLog(t *testing.T) {
	n := 1000000
	paths := makepaths3()
	mi, _ := makeLLRB(n)
	defer mi.Destroy()

	t.Logf("paths %v, entries: %v", paths, n)

	rand.Seed(time.Now().UnixNano())
	name, msize := "testbuild", int64(4096)
	zsize := []int64{0, msize, msize * 2}[rand.Intn(100000)%2]
	vsize := []int64{0, zsize, zsize * 2}[rand.Intn(100000)%2]
	mmap := []bool{false, true}[rand.Intn(10000)%2]
	t.Logf("zsize: %v, vsize: 0, mmap: %v", zsize, mmap)
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

	snap.Log()
}

func BenchmarkSnapCount(b *testing.B) {
	snap, _ := makeBubt(10000)
	defer snap.Destroy()
	defer snap.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		snap.Count()
	}
}

func BenchmarkSnapGet(b *testing.B) {
	n := b.N
	if n < 1000 {
		n = 1000
	}
	snap, keys := makeBubt(n)
	defer snap.Destroy()
	defer snap.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		snap.Get(keys[i], nil)
	}
}

func BenchmarkSnapScan(b *testing.B) {
	n := b.N
	if n < 1000 {
		n = 1000
	}
	snap, _ := makeBubt(n)
	defer snap.Destroy()
	defer snap.Close()

	iter := snap.Scan()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		iter(false /*fin*/)
	}
	iter(true /*fin*/)
}

func BenchmarkSnapView(b *testing.B) {
	n := b.N
	if n < 1000 {
		n = 1000
	}
	snap, _ := makeBubt(n)
	defer snap.Destroy()
	defer snap.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		snap.View(0x1).Abort()
	}
}

func BenchmarkViewGet(b *testing.B) {
	n := b.N
	if n < 1000 {
		n = 1000
	}
	snap, keys := makeBubt(n)
	defer snap.Destroy()
	defer snap.Close()

	view := snap.View(1)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		view.Get(keys[i], nil)
	}
	view.Abort()
}

func BenchmarkViewCursor(b *testing.B) {
	n := b.N
	if n < 1000 {
		n = 1000
	}
	snap, _ := makeBubt(n)
	defer snap.Destroy()
	defer snap.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		view := snap.View(0x1)
		view.OpenCursor(nil)
		view.Abort()
	}
}

func BenchmarkCursorKey(b *testing.B) {
	n := 1000
	if n < 1000 {
		n = 1000
	}
	snap, _ := makeBubt(n)
	defer snap.Destroy()
	defer snap.Close()

	view := snap.View(0x1)
	x, _ := view.OpenCursor(nil)
	cur := x.(*Cursor)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cur.Key()
		cur.getnext()
	}
	view.Abort()
}

func BenchmarkCursorValue(b *testing.B) {
	n := 1000
	if n < 1000 {
		n = 1000
	}
	snap, _ := makeBubt(n)
	defer snap.Destroy()
	defer snap.Close()

	view := snap.View(0x1)
	x, _ := view.OpenCursor(nil)
	cur := x.(*Cursor)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cur.Value()
		cur.getnext()
	}
	view.Abort()
}

func BenchmarkCursorGetNext(b *testing.B) {
	snap, _ := makeBubt(b.N)
	defer snap.Destroy()
	defer snap.Close()

	view := snap.View(0x1)
	cur, _ := view.OpenCursor(nil)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cur.GetNext()
	}
	view.Abort()
}

func BenchmarkCursorYNext(b *testing.B) {
	snap, _ := makeBubt(b.N)
	defer snap.Destroy()
	defer snap.Close()

	view := snap.View(0x1)
	cur, _ := view.OpenCursor(nil)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cur.YNext(false /*fin*/)
	}
	view.Abort()
}

func makeBubt(n int) (*Snapshot, [][]byte) {
	mi, keys := makeLLRB(n)
	defer mi.Destroy()

	name, paths := "benchbubt", makepaths3()
	bubt, err := NewBubt(name, paths, 4096 /*msize*/, 4096 /*zsize*/, 0)
	if err != nil {
		panic(err)
	}
	miter := mi.Scan()
	if err := bubt.Build(miter, []byte("this is metadata")); err != nil {
		panic(err)
	}
	miter(true /*fin*/)
	bubt.Close()

	snap, err := OpenSnapshot(name, paths, true /*mmap*/)
	if err != nil {
		panic(err)
	}
	return snap, keys
}
