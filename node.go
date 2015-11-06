package llrb

import "unsafe"

// 56 byte overhead.
type node struct {
	hdr1   uint64 // dirty, black, ksize(12), blksize(20)
	pool   *mempool
	left   *node
	right  *node
	mvalue unsafe.Pointer
	fvalue int64  // file-position
	stat1  uint64 // ts[:48]
	// key
}

//---- field operations

func (nd *node) setblocksize(size int) *node {
	nd.hdr1 = (nd.hdr1 & 0xfffffffffff00000) | (uint64(size) & 0xfffff)
	return nd
}

func (nd *node) blocksize() int {
	return int(nd.hdr1 & 0xfffff)
}

func (nd *node) setkeysize(size int) *node {
	nd.hdr1 = (nd.hdr1 & 0xffffffff000fffff) | ((uint64(size) & 0x3ff) << 20)
	return nd
}

func (nd *node) keysize() int {
	return int((nd.hdr1 & 0x3ff00000) >> 20)
}

func (nd *node) isred() bool {
	return (nd.hdr1 & 0x100000000) == 0
}

func (nd *node) isblack() bool {
	return !nd.isred()
}

func (nd *node) setblack() *node {
	nd.hdr1 = nd.hdr1 | 0x100000000
	return nd
}

func (nd *node) setred() *node {
	nd.hdr1 = nd.hdr1 & 0xfffffffeffffffff
	return nd
}

func (nd *node) togglelink() *node {
	nd.hdr1 = nd.hdr1 ^ 0x100000000
	return nd
}

func (nd *node) isdirty() bool {
	return (nd.hdr1 & 0x200000000) == 1
}

func (nd *node) setdirty() *node {
	nd.hdr1 = nd.hdr1 | 0x200000000
	return nd
}

func (nd *node) cleardirty() *node {
	nd.hdr1 = nd.hdr1 & 0xfffffffdffffffff
	return nd
}

func (nd *node) settimestamp(ts int64) *node {
	nd.stat1 = (nd.hdr1 & 0xffff000000000000) | (uint64(ts) & 0xffffffffffff)
	return nd
}

func (nd *node) timestamp() int64 {
	return int64(nd.stat1 & 0xffffffffffff)
}

//---- tree operations

func rotateleft(h *node) *node {
	x := h.right
	if x.isblack() {
		panic("rotating a black link")
	}
	h.right = x.left
	x.left = h
	if h.isblack() {
		x.setblack()
	} else {
		x.setred()
	}
	h.setred()
	return x
}

func rotateright(h *node) *node {
	x := h.left
	if x.isblack() {
		panic("rotating a black link")
	}
	h.left = x.right
	x.right = h
	if h.isblack() {
		x.setblack()
	} else {
		x.setred()
	}
	h.setred()
	return x
}

func flip(h *node) {
	h.togglelink()
	h.left.togglelink()
	h.right.togglelink()
}

func moveredleft(h *node) *node {
	flip(h)
	if h.right.left.isred() {
		h.right = rotateright(h.right)
		h = rotateleft(h)
		flip(h)
	}
	return h
}

func moveredright(h *node) *node {
	flip(h)
	if h.left.left.isred() {
		h = rotateright(h)
		flip(h)
	}
	return h
}

func fixUp(h *node) *node {
	if h.right.isred() {
		h = rotateleft(h)
	}
	if h.left.isred() && h.left.left.isred() {
		h = rotateright(h)
	}
	if h.left.isred() && h.right.isred() {
		flip(h)
	}
	return h
}
