package llrb

import "bytes"
import "testing"

func TestScanner(t *testing.T) {
	scanlimit = 2
	sb := makescanbuf()
	testdata := [][]interface{}{
		{[]byte("key1"), []byte("value1"), uint64(1), false},
		{[]byte("key2"), []byte("value2"), uint64(2), true},
		{[]byte("key3"), []byte("value3"), uint64(3), false},
		{[]byte("key4"), []byte("value4"), uint64(4), true},
		{[]byte("key5"), []byte("value5"), uint64(5), false},
		{[]byte("key6"), []byte("value6"), uint64(6), true},
		{[]byte("key7"), []byte("value7"), uint64(7), false},
		{[]byte("key8"), []byte("value8"), uint64(8), true},
	}
	verify := func(from, till int) {
		i := from
		sb.startread()
		key, val, seqno, deleted := sb.pop()
		for key != nil {
			tdata := testdata[i]
			refkey, refval := tdata[0].([]byte), tdata[1].([]byte)
			refseqno, refdeleted := tdata[2].(uint64), tdata[3].(bool)
			if bytes.Compare(key, refkey) != 0 {
				t.Errorf("expected %s, got %s", refkey, key)
			} else if bytes.Compare(val, refval) != 0 {
				t.Errorf("expected %s, got %s", refval, val)
			} else if seqno != refseqno {
				t.Errorf("expected %v, got %v", refseqno, seqno)
			} else if deleted != refdeleted {
				t.Errorf("expected %v, got %v", refdeleted, deleted)
			}
			key, val, seqno, deleted = sb.pop()
			i++
		}
		if i != (till + 1) {
			t.Errorf("expected %v, got %v", till+1, i)
		}
	}

	sb.startwrite()
	from := 0
	for till, tdata := range testdata {
		key, val := tdata[0].([]byte), tdata[1].([]byte)
		seqno, deleted := tdata[2].(uint64), tdata[3].(bool)
		if n := sb.append(key, val, seqno, deleted); n >= scanlimit {
			verify(from, till)
			sb.startwrite()
			from = till + 1
		}
	}
}
