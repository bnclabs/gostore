package bubt

import "fmt"
import "testing"

func BenchmarkMGetIndex(b *testing.B) {
	m, _ := makemsnap(b)
	index := make(blkindex, 0, 1024)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.getindex(index)
	}
}

func BenchmarkMFindkey(b *testing.B) {
	m, keys := makemsnap(b)
	index := make(blkindex, 0, 1024)
	index = m.getindex(index)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.findkey(0, index, keys[i%len(keys)])
	}
}

func makemsnap(tb testing.TB) (msnap, [][]byte) {
	mblocksize := int64(4 * 1024)

	m, keys := newm(nil, mblocksize), [][]byte{}
	i := 0
	k, vpos := fmt.Sprintf("%16d", i), (((i % 4) << 56) | i)
	for m.insert([]byte(k), int64(vpos)) {
		keys = append(keys, []byte(k))
		//tb.Logf("insert %s", k)
		i++
		k, vpos = fmt.Sprintf("%16d", i), (((i % 4) << 56) | i)
	}
	if m.finalize() == false {
		tb.Errorf("unexpected false")
	}
	return msnap(m.block), keys
}
