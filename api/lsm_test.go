package api

import "bytes"
import "sort"
import "time"
import "fmt"
import "math/rand"
import "testing"

var _ = fmt.Sprintf("dummy")

func TestLSMIterators(t *testing.T) {
	arrsz := 4
	for n := uint64(1); n < 15; n++ {
		arrs := make([]bytesarray, n)
		iters := make([]IndexIterator, n)
		arr := make(bytesarray, 0)
		keys := makekeys(arrsz, 10)
		for x := uint64(0); x < n; x++ {
			arrs[x] = newTestarray(keys, x+1)
			sort.Sort(arrs[x])
			iters[x] = arrs[x].iterate()
		}
		arr = append(arr, arrs[n-1]...)
		sort.Sort(arr)
		iter := LSMIterators(false, iters...)
		for _, ref := range arr {
			node := iter.Next()
			bornseqno, deadseqno := node.Bornseqno(), node.Deadseqno()
			if bytes.Compare(ref.key, node.Key()) != 0 {
				fmsg := "expected %v, got %v"
				t.Fatalf(fmsg, string(ref.key), string(node.Key()))
			} else if !(bornseqno == n || deadseqno == n) {
				t.Fatalf("expected %v, got {%v,%v}", n, bornseqno, deadseqno)
			}
		}
	}
}

func TestLSMReverse(t *testing.T) {
	arrsz := 4
	for n := uint64(1); n < 15; n++ {
		arrs := make([]bytesarray, n)
		iters := make([]IndexIterator, n)
		arr := make(bytesarray, 0)
		keys := makekeys(arrsz, 10)
		for x := uint64(0); x < n; x++ {
			arrs[x] = newTestarray(keys, x+1)
			sort.Sort(sort.Reverse(arrs[x]))
			iters[x] = arrs[x].iterate()
		}
		arr = append(arr, arrs[n-1]...)
		sort.Sort(sort.Reverse(arr))
		iter := LSMIterators(true, iters...)
		for _, ref := range arr {
			node := iter.Next()
			bornseqno, deadseqno := node.Bornseqno(), node.Deadseqno()
			if bytes.Compare(ref.key, node.Key()) != 0 {
				fmsg := "expected %v, got %v"
				t.Fatalf(fmsg, string(ref.key), string(node.Key()))
			} else if !(bornseqno == n || deadseqno == n) {
				t.Fatalf("expected %v, got {%v,%v}", n, bornseqno, deadseqno)
			}
		}
	}
}

func BenchmarkLSM(b *testing.B) {
	arrsz := 100
	n := uint64(4)
	arrs := make([]bytesarray, n)
	iters := make([]IndexIterator, n)
	arr := make(bytesarray, 0)
	keys := makekeys(arrsz, 10)
	for x := uint64(0); x < n; x++ {
		arrs[x] = newTestarray(keys, x+1)
		sort.Sort(arrs[x])
		iters[x] = arrs[x].iterate()
		arr = append(arr, arrs[x]...)
	}
	sort.Sort(arr)

	for i := 0; i < b.N; i++ {
		iter := LSMIterators(false, iters...)
		for nd := iter.Next(); nd != nil; nd = iter.Next() {
		}
	}
}

type bytesarray []*bytesnode

func newTestarray(keys [][]byte, num uint64) bytesarray {
	a := make([]*bytesnode, len(keys))
	for i := range a {
		a[i] = &bytesnode{key: keys[i]}
		if rand.Intn(2) == 0 {
			a[i].bornseqno = num
		} else {
			a[i].deadseqno = num
		}
	}
	return a
}

func (a bytesarray) iterate() IndexIterator {
	return &bytesarrayiter{a, 0}
}

func (a bytesarray) Len() int {
	return len(a)
}

func (a bytesarray) Less(i, j int) bool {
	ai, aj := a[i], a[j]
	cmp := bytes.Compare(ai.key, aj.key)
	if cmp != 0 {
		return cmp < 0
	}
	seqnoi := ai.bornseqno
	if ai.bornseqno < ai.deadseqno {
		seqnoi = ai.deadseqno
	}
	seqnoj := aj.bornseqno
	if aj.bornseqno < aj.deadseqno {
		seqnoj = aj.deadseqno
	}
	return seqnoi < seqnoj
}

func (a bytesarray) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

type bytesarrayiter struct {
	a    bytesarray
	next int
}

func (iter *bytesarrayiter) Next() Node {
	if iter.next >= len(iter.a) {
		return nil
	}
	n := iter.a[iter.next]
	iter.next++
	return n
}

func (iter *bytesarrayiter) Close() {
}

func makekeys(count, size int) [][]byte {
	keys := make([][]byte, count)
	for i := range keys {
		keys[i] = makebytes(make([]byte, size), size)
		keys[i] = append(keys[i], fmt.Sprintf("%v", i)...)
		time.Sleep(1 * time.Millisecond)
	}
	return keys
}

func makebytes(buf []byte, size int) []byte {
	ln := time.Now().UnixNano()
	b, from := buf[:0], byte(ln%26)
	for i := 0; i < size; i++ {
		b = append(b, 97+((from+byte(i))%26))
	}
	return b
}

type bytesnode struct {
	key       []byte
	bornseqno uint64
	deadseqno uint64
}

func (n *bytesnode) Vbno() (vbno uint16) {
	panic("unsupported call")
}

func (n *bytesnode) Access() (ts uint64) {
	panic("unsupported call")
}

func (n *bytesnode) Bornseqno() (seqno uint64) {
	return n.bornseqno
}

func (n *bytesnode) Deadseqno() (seqno uint64) {
	return n.deadseqno
}

func (nd *bytesnode) IsDeleted() bool {
	panic("unsupported call")
}

func (n *bytesnode) Vbuuid() (uuid uint64) {
	panic("unsupported call")
}

func (n *bytesnode) Fpos() (level byte, offset int64) {
	panic("unsupported call")
}

func (n *bytesnode) Key() (key []byte) {
	return n.key
}

func (n *bytesnode) Value() (value []byte) {
	panic("unsupported call")
}

func (n *bytesnode) Setvbno(vbno uint16) Node {
	panic("unsupported call")
}

func (n *bytesnode) Setaccess(access uint64) Node {
	panic("unsupported call")
}

func (n *bytesnode) SetVbuuid(uuid uint64) Node {
	panic("unsupported call")
}

func (n *bytesnode) SetFpos(level byte, offset uint64) Node {
	panic("unsupported call")
}

func (n *bytesnode) SetBornseqno(seqno uint64) Node {
	n.bornseqno = seqno
	return n
}

func (n *bytesnode) SetDeadseqno(seqno uint64) Node {
	n.deadseqno = seqno
	return n
}
