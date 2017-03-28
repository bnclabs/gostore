package malloc

import "unsafe"

import "github.com/prataprc/gostore/lib"

// should always be power of 2.
const cacheline = 64

type freebits struct {
	cacheline int64
	cachemask int64
	nblocks   int64
	freeoffs  []int64
	bitmaps   [][]uint8
}

func newfreebits(cacheline int64, nblocks int64) *freebits {
	if (nblocks & 0x7) != 0 {
		panic("should be multiples of 8")
	}

	fbits := &freebits{
		cacheline: cacheline, cachemask: cacheline - 1, nblocks: nblocks,
	}
	fbits.bitmaps = fbits.fillbits(nblocks, make([][]byte, 0))
	fbits.freeoffs = make([]int64, len(fbits.bitmaps))
	return fbits
}

func (fbits *freebits) fillbits(bits int64, bitmaps [][]uint8) [][]uint8 {
	bitmap, reducedbits := fbits.initbits(bits)
	if bits > (fbits.cacheline << 3) {
		bitmaps = fbits.fillbits(reducedbits, bitmaps)
		bitmaps = append(bitmaps, bitmap)
		return bitmaps
	}
	return append(bitmaps, bitmap)
}

func (fbits *freebits) initbits(bits int64) ([]uint8, int64) {
	bitmap := make([]uint8, ceil(bits, 8))
	for i := int64(0); i < (bits >> 3); i++ {
		bitmap[i] = 0xff
	}
	if x := (bits & 0x7); x > 0 {
		byt := uint8(0)
		for i := int64(0); i < x; i++ {
			byt = lib.Bit8(byt).Setbit(uint8(i))
		}
		bitmap[len(bitmap)-1] = byt
	}
	reducedbits := ceil(int64(len(bitmap)), fbits.cacheline)
	return bitmap, reducedbits
}

func (fbits *freebits) sizeof() int64 {
	sz := int64(unsafe.Sizeof(*fbits))
	for _, slice := range fbits.bitmaps {
		sz += int64(len(slice))
	}
	return sz
}

func (fbits *freebits) freeblocks() (n int64) {
	ln := len(fbits.bitmaps)
	for _, byt := range fbits.bitmaps[ln-1] {
		n += int64(lib.Bit8(byt).Ones())
	}
	return
}

func (fbits *freebits) alloc() (int64, bool) {
	nthblock, fin := fbits.doalloc(0, 0)
	return nthblock, fin
}

func (fbits *freebits) doalloc(off, index int64) (int64, bool) {
	bmap := fbits.bitmaps[index]
	if x := fbits.freeoffs[index]; x >= 0 {
		off += x
	}
	for i, byt := range bmap[off:] {
		if byt == 0 {
			continue
		}
		n := lib.Bit8(byt).Findfirstset()

		bit := ((off + int64(i)) << 3) + int64(n)
		if (index + 1) == int64(len(fbits.bitmaps)) { // terminating condition
			bmap[off+int64(i)] = lib.Bit8(byt).Clearbit(uint8(n))
			if bit == (fbits.nblocks - 1) {
				fbits.freeoffs[index] = -1
				return bit, true
			}
			return bit, false
		}

		nthblock, fin := fbits.doalloc(bit*fbits.cacheline, index+1)
		if fin {
			bmap[off+int64(i)] = lib.Bit8(byt).Clearbit(uint8(n))
			if (n == 7) && (int64(i) == (fbits.cacheline - 1)) {
				fbits.freeoffs[index] = -1
				return nthblock, true
			}
			return nthblock, false
		}
		return nthblock, false
	}
	return -1, true
}

func (fbits *freebits) free(nthblock int64) {
	fbits.dofree(nthblock, 0)
}

func (fbits *freebits) dofree(nthblock, index int64) int64 {
	bmap := fbits.bitmaps[index]
	if (index + 1) == int64(len(fbits.bitmaps)) {
		q, r := (nthblock >> 3), (nthblock & 0x7)
		off, byt := q&fbits.cachemask, bmap[q]
		bmap[q] = lib.Bit8(byt).Setbit(uint8(r))
		if fbits.freeoffs[index] < 0 || off < fbits.freeoffs[index] {
			fbits.freeoffs[index] = off
		}
		return nthblock / (fbits.cacheline << 3)
	}
	bit := fbits.dofree(nthblock, index+1)
	q, r := (bit >> 3), (bit & 0x7)
	off, byt := q&fbits.cachemask, bmap[q]
	bmap[q] = lib.Bit8(byt).Setbit(uint8(r))
	if fbits.freeoffs[index] < 0 || off < fbits.freeoffs[index] {
		fbits.freeoffs[index] = off
	}
	return bit / (fbits.cacheline << 3)
}

func ceil(divident, divisor int64) int64 {
	if divident%divisor == 0 {
		return divident / divisor
	}
	return (divident / divisor) + 1
}
