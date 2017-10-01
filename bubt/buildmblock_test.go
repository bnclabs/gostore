package bubt

import "fmt"
import "testing"

func TestMBlock(t *testing.T) {
	mblocksize := int64(4 * 1024)

	m := newm(mblocksize)
	if m.finalize() == true {
		t.Errorf("unexpected true")
	}

	i := 0
	k, vpos := fmt.Sprintf("%16d", i), (((i % 4) << 56) | i)
	for m.insert([]byte(k), int64(vpos)) {
		//t.Logf("insert %s", k)
		i++
		k, vpos = fmt.Sprintf("%16d", i), (((i % 4) << 56) | i)
	}
	t.Logf("Inserted %v items", i)

	if m.finalize() == false {
		t.Errorf("unexpected false")
	}
	if int64(len(m.block)) != mblocksize {
		t.Errorf("expected %v, got %v", len(m.block), mblocksize)
	}

	// verify.
	ms := msnap(m.block)
	index := ms.getindex(blkindex{})
	j, k := 0, fmt.Sprintf("%16d", 0)
	for j < i {
		level, fpos := ms.findkey(0, index, []byte(k))
		if level != byte(j%4) {
			t.Errorf("expected %v, got %v", j%4, level)
		} else if fpos != int64(j) {
			t.Errorf("expected %v, got %v", j, fpos)
		}
		j++
		k = fmt.Sprintf("%16d", j)
	}

	level, fpos := ms.findkey(0, index, []byte(fmt.Sprintf("%17d", 100)))
	if level != 2 {
		t.Errorf("expected %v, got %v", 2, level)
	} else if fpos != 10 {
		t.Errorf("expected %v, got %v", 10, fpos)
	}
}

func BenchmarkMInsert(b *testing.B) {
	blocksize := int64(4096)
	k, vpos := []byte("aaaaaaaaaaaaaaaaaaaaaaa"), int64(1023)
	m := newm(blocksize)
	for i := 0; i < b.N; i++ {
		if m.insert(k, vpos) == false {
			m.firstkey = m.firstkey[:0]
			m.index = m.index[:0]
			m.buffer = m.buffer[0 : 2*blocksize]
			m.entries = m.buffer[blocksize:blocksize]
			if m.insert(k, vpos) == false {
				panic("unexpected")
			}
		}
	}
}
