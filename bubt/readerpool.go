package bubt

import "unsafe"
import "runtime"
import "sync/atomic"

type readerpool struct {
	head     unsafe.Pointer // *readbuffers
	spinlock int64
	n        int64
	max      int64
}

func newreaderpool(msize, zsize, vsize, max int64) *readerpool {
	pool := &readerpool{head: nil, spinlock: 0, n: 0, max: max}
	for i := int64(0); i < max; i++ {
		pool.putreadbuffer(pool.getreadbuffer(msize, zsize, vsize))
	}
	return pool
}

type readbuffers struct {
	index  blkindex
	zblock []byte
	mblock []byte
	vblock []byte
	next   unsafe.Pointer // *readbuffers
}

func (pool *readerpool) getreadbuffer(msize, zsize, vsize int64) *readbuffers {
	for {
		if atomic.CompareAndSwapInt64(&pool.spinlock, 0, 1) {
			if pool.head == nil && pool.n < pool.max {
				atomic.StoreInt64(&pool.spinlock, 0)
				return &readbuffers{
					index:  make(blkindex, 0, 256),
					mblock: make([]byte, msize),
					zblock: make([]byte, zsize),
					vblock: make([]byte, vsize),
				}

			} else if pool.head != nil {
				old := (*readbuffers)(pool.head)
				pool.head = old.next
				atomic.StoreInt64(&pool.spinlock, 0)
				return old
			}
			// wait, till buffer returns to pool
			atomic.StoreInt64(&pool.spinlock, 0)
		}
		runtime.Gosched()
	}
}

func (pool *readerpool) putreadbuffer(b *readbuffers) {
	for {
		if atomic.CompareAndSwapInt64(&pool.spinlock, 0, 1) {
			b.next = pool.head
			pool.head = unsafe.Pointer(b)
			atomic.StoreInt64(&pool.spinlock, 0)
			return
		}
		runtime.Gosched()
	}
}
