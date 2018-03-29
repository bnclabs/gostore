package bubt

import "unsafe"
import "runtime"
import "sync/atomic"

import "github.com/bnclabs/gostore/lib"

type blockpool struct {
	head unsafe.Pointer // *blockdata
	n    int64
	max  int64
}

func newblockpool(max int64) *blockpool {
	return &blockpool{head: nil, n: 0, max: max}
}

type blockdata struct {
	data []byte
	next unsafe.Pointer // *blockdata
}

func (pool *blockpool) getblock(size int) *blockdata {
loop:
	for {
		oldptr := atomic.LoadPointer(&pool.head)
		if oldptr == nil {
			if atomic.AddInt64(&pool.n, 1) > pool.max {
				atomic.AddInt64(&pool.n, -1)
				runtime.Gosched()
				continue loop
			}
			b := &blockdata{data: lib.Fixbuffer(nil, int64(size)), next: nil}
			return b
		}
		old := (*blockdata)(oldptr)
		nextptr := atomic.LoadPointer(&old.next)
		if atomic.CompareAndSwapPointer(&pool.head, oldptr, nextptr) {
			old.data = lib.Fixbuffer(old.data, int64(size))
			return old
		}
	}
}

func (pool *blockpool) putblock(b *blockdata) {
	var newptr unsafe.Pointer

	for {
		oldptr := atomic.LoadPointer(&pool.head)
		b.next, newptr = oldptr, unsafe.Pointer(b)
		if atomic.CompareAndSwapPointer(&pool.head, oldptr, newptr) {
			return
		}
	}
}
