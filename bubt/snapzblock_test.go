package bubt

import "fmt"
import "testing"

func TestZGetNext(t *testing.T) {
	z, keys := makezsnap(t)
	for i := range keys {
		for j := i + 1; j < len(keys); j++ {
			key, lv, seqno, deleted := z.getnext(j - 1)
			value, _ := lv.getactual(nil, nil)
			if string(key) != string(keys[j]) {
				t.Errorf("expected %q, got %q", keys[j], key)
			} else if string(value) != string(keys[j]) {
				t.Errorf("expected %q, got %q", keys[j], value)
			} else if seqno != uint64(j) {
				t.Errorf("expected %v, got %v", j, seqno)
			} else if deleted != ((j % 4) == 0) {
				t.Errorf("expected %v, got %v", (j%4) == 0, deleted)
			}
		}
	}
}

func BenchmarkZGetIndex(b *testing.B) {
	z, _ := makezsnap(b)
	index := make(blkindex, 0, 1024)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		z.getindex(index)
	}
}

func BenchmarkZFindkey(b *testing.B) {
	z, keys := makezsnap(b)
	index := make(blkindex, 0, 1024)
	index = z.getindex(index)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		z.findkey(0, index, keys[i%len(keys)])
	}
}

func BenchmarkZGetNext(b *testing.B) {
	z, _ := makezsnap(b)

	b.ResetTimer()
	index := 0
	for i := 0; i < b.N; i++ {
		if key, _, _, _ := z.getnext(index); key == nil {
			index = 0
			if key, _, _, _ = z.getnext(index); key != nil {
				panic("unexpected")
			}
		}
	}
}

func makezsnap(tb testing.TB) (zsnap, [][]byte) {
	zblocksize := int64(4 * 1024)

	z, keys := newz(zblocksize, -1, 0, nil), [][]byte{}
	i := uint64(0)
	k := fmt.Sprintf("%16d", i)
	v, seqno, deleted := k, i, true
	for z.insert([]byte(k), []byte(v), seqno, deleted) {
		keys = append(keys, []byte(k))
		i++
		k = fmt.Sprintf("%16d", i)
		v, seqno, deleted = k, i, (i%4) == 0
	}
	if z.finalize() == false {
		tb.Errorf("unexpected false")
	}
	return zsnap(z.block), keys
}
