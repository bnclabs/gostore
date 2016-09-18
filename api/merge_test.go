package api

import "bytes"
import "sort"
import "fmt"
import "math/rand"
import "testing"

var _ = fmt.Sprintf("dummy")

func TestMergeIterators(t *testing.T) {
	for n := 0; n < 15; n++ {
		arrs := make([]bytesarray, n)
		iters := make([]IndexIterator, n)
		arr := make(bytesarray, 0)
		for x := 0; x < n; x++ {
			arrs[x] = newTestarray(64, rand.Intn(10))
			sort.Sort(arrs[x])
			iters[x] = arrs[x].iterate()
			arr = append(arr, arrs[x]...)
		}
		iter := MergeIterators(iters...)
		sort.Sort(arr)
		for _, ref := range arr {
			key := iter.Next().Key()
			if bytes.Compare(ref, key) != 0 {
				fmsg := "expected %v, got %v"
				t.Fatalf(fmsg, string(ref), string(key))
			}
		}
	}
}

func BenchmarkMerge(b *testing.B) {
	n := 6
	arrs := make([]bytesarray, n)
	iters := make([]IndexIterator, n)
	arr := make(bytesarray, 0)
	for x := 0; x < n; x++ {
		arrs[x] = newTestarray(64, 30)
		sort.Sort(arrs[x])
		iters[x] = arrs[x].iterate()
		arr = append(arr, arrs[x]...)
	}
	sort.Sort(arr)

	for i := 0; i < b.N; i++ {
		iter := MergeIterators(iters...)
		for nd := iter.Next(); nd != nil; nd = iter.Next() {
		}
	}
}

type bytesarray []bytesnode

func newTestarray(size, count int) bytesarray {
	a := make([]bytesnode, count)
	for i := range a {
		a[i] = makebytes(make(bytesnode, size), size)
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
	return bytes.Compare(a[i], a[j]) < 0
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

func makebytes(buf []byte, size int) []byte {
	ln := rand.Intn(size)
	b, from := buf[:0], byte(ln%26)
	for i := 0; i < ln; i++ {
		b = append(b, 97+((from+byte(i))%26))
	}
	return b
}

type bytesnode []byte

func (n bytesnode) Vbno() (vbno uint16) {
	panic("unsupported call")
}

func (n bytesnode) Access() (ts uint64) {
	panic("unsupported call")
}

func (n bytesnode) Bornseqno() (seqno uint64) {
	panic("unsupported call")
}

func (n bytesnode) Deadseqno() (seqno uint64) {
	panic("unsupported call")
}

func (nd bytesnode) IsDeleted() bool {
	panic("unsupported call")
}

func (n bytesnode) Vbuuid() (uuid uint64) {
	panic("unsupported call")
}

func (n bytesnode) Fpos() (level byte, offset int64) {
	panic("unsupported call")
}

func (n bytesnode) Key() (key []byte) {
	return []byte(n)
}

func (n bytesnode) Value() (value []byte) {
	panic("unsupported call")
}

func (n bytesnode) Setvbno(vbno uint16) Node {
	panic("unsupported call")
}

func (n bytesnode) Setaccess(access uint64) Node {
	panic("unsupported call")
}

func (n bytesnode) SetVbuuid(uuid uint64) Node {
	panic("unsupported call")
}

func (n bytesnode) SetFpos(level byte, offset uint64) Node {
	panic("unsupported call")
}

func (n bytesnode) SetBornseqno(seqno uint64) Node {
	panic("unsupported call")
}

func (n bytesnode) SetDeadseqno(seqno uint64) Node {
	panic("unsupported call")
}
