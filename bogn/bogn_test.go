package bogn

import "io"
import "fmt"
import "testing"
import "time"
import "sync"
import "sync/atomic"
import "math/rand"

import "github.com/prataprc/gostore/llrb"

func TestReload(t *testing.T) {
	destoryindex("index", makepaths())

	mindex := llrb.NewLLRB("mindex", llrb.Defaultsettings())
	setts, paths := makesettings(), makepaths()
	t.Log(paths)
	setts["bubt.diskpaths"] = paths
	index, err := New("index", setts)
	if err != nil {
		t.Fatal(err)
	}
	index.Start()

	n := 10000
	k, v := []byte("key000000000000"), []byte("val00000000000000")
	for i := 0; i < n; i++ {
		x := fmt.Sprintf("%d", i)
		key, val := append(k[:3], x...), append(v[:3], x...)
		mindex.Set(key, val, nil)
		index.Set(key, val, nil)
		if i%10 == 0 {
			mindex.Delete(key, nil, true /*lsm*/)
			index.Delete(key, nil, true /*lsm*/)
		}
	}
	t.Logf("Loaded %v items", n)

	w := time.Duration(setts.Int64("llrb.snapshottick")) * time.Millisecond
	w *= 100
	time.Sleep(w)

	miter := mindex.Scan()
	iter := index.Scan()
	key1, val1, seqno1, del1, err1 := miter(false /*fin*/)
	key2, val2, seqno2, del2, err2 := iter(false /*fin*/)
	for err1 == nil && err2 == nil {
		if string(key1) != string(key2) {
			t.Errorf("expected %q, got %q", key1, key2)
		} else if string(val1) != string(val2) {
			t.Errorf("%q expected %q, got %q", key1, val1, val2)
		} else if seqno1 != seqno2 {
			t.Errorf("%q expected %q, got %q", key1, seqno1, seqno2)
		} else if del1 != del2 {
			t.Errorf("%q expected %q, got %q", key1, del1, del2)
		}
		key1, val1, seqno1, del1, err1 = miter(false /*fin*/)
		key2, val2, seqno2, del2, err2 = iter(false /*fin*/)
	}
	if err1 != io.EOF || err2 != io.EOF {
		t.Errorf("unexpected %v %v", err1, err2)
	}
	t.Logf("iteration successful")

	index.Close()

	//// Reload
	setts, paths = makesettings(), makepaths()
	setts["bubt.diskpaths"] = paths
	index, err = New("index", setts)
	if err != nil {
		t.Fatal(err)
	}
	index.Start()

	miter = mindex.Scan()
	iter = index.Scan()
	key1, val1, seqno1, del1, err1 = miter(false /*fin*/)
	key2, val2, seqno2, del2, err2 = iter(false /*fin*/)
	for err1 == nil && err2 == nil {
		if string(key1) != string(key2) {
			t.Errorf("expected %q, got %q", key1, key2)
		} else if string(val1) != string(val2) {
			t.Errorf("%q expected %q, got %q", key1, val1, val2)
		} else if seqno1 != seqno2 {
			t.Errorf("%q expected %q, got %q", key1, seqno1, seqno2)
		} else if del1 != del2 {
			t.Errorf("%q expected %q, got %q", key1, del1, del2)
		}
		key1, val1, seqno1, del1, err1 = miter(false /*fin*/)
		key2, val2, seqno2, del2, err2 = iter(false /*fin*/)
	}
	if err1 != io.EOF || err2 != io.EOF {
		t.Errorf("unexpected %v %v", err1, err2)
	}

	index.Close()
	index.Destroy()

	t.Logf("reload and iteration successful")
}

func TestReloadDurable(t *testing.T) {
	destoryindex("index", makepaths())

	mindex := llrb.NewLLRB("mindex", llrb.Defaultsettings())
	setts, paths := makesettings(), makepaths()
	t.Log(paths)
	setts["bubt.diskpaths"] = paths
	setts["durable"] = true
	index, err := New("index", setts)
	if err != nil {
		t.Fatal(err)
	}
	index.Start()

	n := 100000
	k, v := []byte("key000000000000"), []byte("val00000000000000")
	for i := 0; i < n; i++ {
		x := fmt.Sprintf("%d", i)
		key, val := append(k[:3], x...), append(v[:3], x...)
		mindex.Set(key, val, nil)
		index.Set(key, val, nil)
		if i%10 == 0 {
			mindex.Delete(key, nil, true /*lsm*/)
			index.Delete(key, nil, true /*lsm*/)
		}
	}
	t.Logf("Loaded %v items", n)

	w := time.Duration(setts.Int64("llrb.snapshottick")) * time.Millisecond
	w *= 100
	time.Sleep(w)

	miter := mindex.Scan()
	iter := index.Scan()
	key1, val1, seqno1, del1, err1 := miter(false /*fin*/)
	key2, val2, seqno2, del2, err2 := iter(false /*fin*/)
	for err1 == nil && err2 == nil {
		if string(key1) != string(key2) {
			t.Errorf("expected %q, got %q", key1, key2)
		} else if string(val1) != string(val2) {
			t.Errorf("%q expected %q, got %q", key1, val1, val2)
		} else if seqno1 != seqno2 {
			t.Errorf("%q expected %q, got %q", key1, seqno1, seqno2)
		} else if del1 != del2 {
			t.Errorf("%q expected %q, got %q", key1, del1, del2)
		}
		key1, val1, seqno1, del1, err1 = miter(false /*fin*/)
		key2, val2, seqno2, del2, err2 = iter(false /*fin*/)
	}
	if err1 != io.EOF || err2 != io.EOF {
		t.Errorf("unexpected %v %v", err1, err2)
	}
	t.Logf("iteration successful")

	index.Close()

	//// Reload
	setts, paths = makesettings(), makepaths()
	setts["bubt.diskpaths"] = paths
	index, err = New("index", setts)
	if err != nil {
		t.Fatal(err)
	}
	index.Start()

	miter = mindex.Scan()
	iter = index.Scan()
	key1, val1, seqno1, del1, err1 = miter(false /*fin*/)
	key2, val2, seqno2, del2, err2 = iter(false /*fin*/)
	for err1 == nil && err2 == nil {
		if string(key1) != string(key2) {
			t.Errorf("expected %q, got %q", key1, key2)
		} else if string(val1) != string(val2) {
			t.Errorf("%q expected %q, got %q", key1, val1, val2)
		} else if seqno1 != seqno2 {
			t.Errorf("%q expected %q, got %q", key1, seqno1, seqno2)
		} else if del1 != del2 {
			t.Errorf("%q expected %q, got %q", key1, del1, del2)
		}
		key1, val1, seqno1, del1, err1 = miter(false /*fin*/)
		key2, val2, seqno2, del2, err2 = iter(false /*fin*/)
	}
	if err1 != io.EOF || err2 != io.EOF {
		t.Errorf("unexpected %v %v", err1, err2)
	}

	index.Close()

	t.Logf("reload and iteration successful")

	//// Re-Reload
	setts, paths = makesettings(), makepaths()
	setts["bubt.diskpaths"] = paths
	index, err = New("index", setts)
	if err != nil {
		t.Fatal(err)
	}
	index.Start()

	miter = mindex.Scan()
	iter = index.Scan()
	key1, val1, seqno1, del1, err1 = miter(false /*fin*/)
	key2, val2, seqno2, del2, err2 = iter(false /*fin*/)
	for err1 == nil && err2 == nil {
		if string(key1) != string(key2) {
			t.Errorf("expected %q, got %q", key1, key2)
		} else if del1 != del2 {
			t.Errorf("%q expected %q, got %q", key1, del1, del2)
		} else if del1 == false && (string(val1) != string(val2)) {
			//fmt.Printf("%q expected %q, got %q\n", key1, val1, val2)
			t.Errorf("%q expected %q, got %q", key1, val1, val2)
		} else if seqno1 != seqno2 {
			t.Errorf("%q expected %q, got %q", key1, seqno1, seqno2)
		}
		key1, val1, seqno1, del1, err1 = miter(false /*fin*/)
		key2, val2, seqno2, del2, err2 = iter(false /*fin*/)
	}
	if err1 != io.EOF || err2 != io.EOF {
		t.Errorf("unexpected %v %v", err1, err2)
	}

	index.Close()
	index.Destroy()

	t.Logf("re-reload and iteration successful")
}

func TestSnaplock(t *testing.T) {
	bogn := &Bogn{}
	buffer := make([]byte, 1000)
	finch := make(chan struct{})
	var wg sync.WaitGroup
	var rcounts [16]int64
	var wcounts [16]int64

	reader := func(i int) {
		defer wg.Done()

		for {
			bogn.snaprlock()
			x := buffer[len(buffer)-1]
			for _, ch := range buffer {
				if ch != x {
					t.Errorf("expected %v, got %v", x, ch)
				}
			}
			bogn.snaprunlock()
			atomic.AddInt64(&rcounts[i], 1)

			select {
			case <-finch:
				return
			default:
			}
		}
	}
	writer := func(i int) {
		defer wg.Done()

		for {
			bogn.snaplock()
			x := byte(rand.Intn(256))
			for i := range buffer {
				buffer[i] = x
			}
			bogn.snapunlock()
			atomic.AddInt64(&wcounts[i], 1)

			select {
			case <-finch:
				return
			default:
			}
		}
	}
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go reader(i)
		if (i % 8) == 0 {
			wg.Add(1)
			go writer(i)
		}
	}
	time.Sleep(10 * time.Second)
	close(finch)
	wg.Wait()

	t.Logf("rcounts: %v", rcounts)
	t.Logf("wcounts: %v", wcounts)
}

// TODO: unit test case
// Open a bogn instance with one level of disk snapshots,
//
