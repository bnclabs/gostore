package malloc

import "fmt"
import "testing"
import "unsafe"
import "sync"
import "reflect"
import "math/rand"
import "sync/atomic"

type testalloc struct {
	n    byte
	size int
	ptr  unsafe.Pointer
}

var ccallocated, ccfreed int64

func TestConcur(t *testing.T) {
	var awg, fwg sync.WaitGroup

	nroutines, repeat := 50, 1000000

	chans := make([]chan testalloc, 0, nroutines)
	for n := 0; n < nroutines; n++ {
		chans = append(chans, make(chan testalloc, 1000))
	}

	capacity := int64(10 * 1024 * 1024 * 1024)
	marena := NewArena(capacity, "flist")
	awg.Add(nroutines)
	fwg.Add(nroutines)
	for n := 0; n < nroutines; n++ {
		go testallocator(marena, byte(n), repeat, chans, &awg)
		go testfree(marena, byte(n), chans[n], &fwg)
	}

	awg.Wait()
	t.Logf("allocations are done\n")

	for _, ch := range chans {
		close(ch)
	}

	fwg.Wait()

	t.Logf("ccallocated:%v ccfreed:%v\n", ccallocated, ccfreed)
	t.Log(marena.Info())
}

func testallocator(
	arena *Arena, n byte, repeat int, chans []chan testalloc, wg *sync.WaitGroup) {

	defer wg.Done()

	var block []byte
	dst := (*reflect.SliceHeader)(unsafe.Pointer(&block))

	slabs := arena.Slabs()[:50]
	src := make([]byte, slabs[len(slabs)-1])
	for i := range src {
		src[i] = n
	}

	for i := 0; i < repeat; i++ {
		size := slabs[rand.Intn(len(slabs))] - 8
		ptr := arena.Alloc(size)

		if x := arena.Slabsize(ptr); x != (size + 8) {
			panic(fmt.Errorf("expected %v, got %v", size+8, x))
		}

		dst.Data, dst.Len, dst.Cap = (uintptr)(ptr), int(size), int(size)
		copy(block, src)

		msg := testalloc{size: int(size), n: n, ptr: ptr}
		chans[rand.Intn(len(chans))] <- msg
		atomic.AddInt64(&ccallocated, size+8)
	}
}

func testfree(arena *Arena, n byte, ch chan testalloc, wg *sync.WaitGroup) {
	defer wg.Done()

	var block []byte
	dst := (*reflect.SliceHeader)(unsafe.Pointer(&block))

	for msg := range ch {
		dst.Data, dst.Len, dst.Cap = (uintptr)(msg.ptr), msg.size, msg.size
		for _, c := range block {
			if c != msg.n {
				panic(fmt.Errorf("expected %v, got %v", msg.n, c))
			}
		}
		arena.Free(msg.ptr)
		atomic.AddInt64(&ccfreed, int64(msg.size+8))
	}
}
