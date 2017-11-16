package bogn

import "io"
import "fmt"
import "testing"

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
