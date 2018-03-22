package bubt

import "fmt"
import "reflect"
import "testing"

func TestZBlock(t *testing.T) {
	zblocksize := int64(4 * 1024)

	z := newz(zblocksize, -1)
	if z.finalize() == true {
		t.Errorf("unexpected true")
	}

	doinsert := func() uint64 {
		i := uint64(0)
		k := fmt.Sprintf("%16d", i)
		v, seqno, deleted := k, i, true
		for z.insert([]byte(k), []byte(v), seqno, deleted) {
			//t.Logf("insert %s", k)
			i++
			k = fmt.Sprintf("%16d", i)
			v, seqno, deleted = k, i, (i%4) == 0
		}
		t.Logf("Inserted %v items", i)
		if z.finalize() == false {
			t.Errorf("unexpected false")
		}
		if int64(len(z.block)) != zblocksize {
			t.Errorf("expected %v, got %v", len(z.block), zblocksize)
		}
		return i
	}

	doverify := func(i uint64) {
		zs := zsnap(z.block)
		index := zs.getindex(blkindex{})
		j, k := uint64(0), fmt.Sprintf("%16d", 0)
		for j < i {
			_, _, lv, seqno, deleted, ok := zs.findkey(0, index, []byte(k))
			value, _ := lv.getactual(nil, nil)
			if ok == false {
				t.Errorf("unexpected false")
			} else if deleted != ((j % 4) == 0) {
				t.Errorf("%q expected %v, got %v", k, ((j % 4) == 0), deleted)
			} else if seqno != j {
				t.Errorf("%q expected %v, got %v", k, j, seqno)
			} else if deleted == false && string(value) != k {
				t.Errorf("expected %s, got %s", k, value)
			}
			j++
			k = fmt.Sprintf("%16d", j)
		}
		k = fmt.Sprintf("%17d", 100)
		idx, _, lv, seqno, deleted, ok := zs.findkey(0, index, []byte(k))
		value, _ := lv.getactual(nil, nil)
		out := []interface{}{idx, value, seqno, deleted, ok}
		ref := []interface{}{11, []byte(nil), uint64(0), false, false}
		if reflect.DeepEqual(ref, out) == false {
			t.Errorf("expected %v, got %v", ref, out)
		}
	}

	doverify(doinsert())
	z.reset(0, nil)
	doverify(doinsert())
}

func BenchmarkZInsert(b *testing.B) {
	blocksize := int64(4096)
	k, value := []byte("aaaaaaaaaaaaaaaaaaaaaaa"), []byte("bbbbbbbbbbbbb")
	z := newz(blocksize, -1)
	for i := 0; i < b.N; i++ {
		if z.insert(k, value, 0, false) == false {
			z.reset(0, nil)
		}
	}
}
