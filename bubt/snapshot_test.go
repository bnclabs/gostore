package bubt

import "testing"

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
	snap, keys := makeBubt(b.N)
	defer snap.Destroy()
	defer snap.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		snap.Get(keys[i], nil)
	}
}

func BenchmarkSnapScan(b *testing.B) {
	snap, _ := makeBubt(b.N)
	defer snap.Destroy()
	defer snap.Close()

	iter := snap.Scan()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		iter(false /*fin*/)
	}
}

func BenchmarkSnapView(b *testing.B) {
	snap, _ := makeBubt(b.N)
	defer snap.Destroy()
	defer snap.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		snap.View(0x1).Abort()
	}
}

func BenchmarkViewGet(b *testing.B) {
	snap, keys := makeBubt(b.N)
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
	snap, _ := makeBubt(b.N)
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
	snap, _ := makeBubt(b.N)
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
	snap, _ := makeBubt(b.N)
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

	name, paths := "benchbubt", makepaths()
	bubt, err := NewBubt(name, paths, 4096 /*msize*/, 4096 /*zsize*/)
	if err != nil {
		panic(err)
	}
	bubt.Build(mi.Scan(), []byte("this is metadata"))
	bubt.Close()

	snap, err := OpenSnapshot(name, paths, true /*mmap*/)
	if err != nil {
		panic(err)
	}
	return snap, keys
}
