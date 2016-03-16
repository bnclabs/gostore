package storage

//import "fmt" // TODO: remove this fmt
import "unsafe"

const cacheline = 64

type freebits struct {
	cacheline int64
	nblocks   int64
	bitmaps   [][]uint8
}

func newfreebits(cacheline int64, nblocks int64) *freebits {
	if (nblocks % 8) != 0 {
		panic("should be multiples of 8")
	}

	fbits := &freebits{cacheline: cacheline, nblocks: nblocks}
	fbits.bitmaps = fbits.fillbits(nblocks, make([][]byte, 0))
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
	if bits%8 > 0 {
		byt := uint8(0)
		for i := int64(0); i < bits%8; i++ {
			byt = setbit8(byt, uint8(i))
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
		n += int64(onesin8(byt))
	}
	return
}

func (fbits *freebits) alloc() (int64, bool) {
	_, nthblock, fin := fbits.doalloc(0, fbits.bitmaps[0], fbits.bitmaps[1:])
	return nthblock, fin
}

func (fbits *freebits) doalloc(
	off int64, bmap []byte, bmaps [][]byte) (int64, int64, bool) {

	for i, byt := range bmap[off:] {
		if byt == 0 {
			continue
		}
		n := findfirstset8(byt)

		bit := ((off + int64(i)) << 3) + int64(n)
		if len(bmaps) == 0 { // terminating condition
			bmap[off+int64(i)] = clearbit8(byt, uint8(n))
			return bit, bit, bit == (fbits.nblocks - 1)
		}
		nbit, nthblock, fin := fbits.doalloc(bit*fbits.cacheline, bmaps[0], bmaps[1:])
		y := nbit - ((bit * fbits.cacheline) << 3)
		if y == (fbits.cacheline<<3) || nthblock >= (fbits.nblocks-1) {
			bmap[off+int64(i)] = clearbit8(byt, uint8(n))
		}
		return bit, nthblock, fin
	}
	return -1, -1, false
}

func (fbits *freebits) free(nthblock int64) {
	fbits.dofree(nthblock, fbits.bitmaps[0], fbits.bitmaps[1:])
}

func (fbits *freebits) dofree(nthblock int64, bmap []uint8, bmaps [][]uint8) int64 {
	if len(bmaps) == 0 {
		q, r := (nthblock >> 3), (nthblock % 8)
		byt := bmap[q]
		bmap[q] = setbit8(byt, uint8(r))
		return nthblock / (fbits.cacheline << 3)
	}
	bit := fbits.dofree(nthblock, bmaps[0], bmaps[1:])
	q, r := (bit >> 3), (bit % 8)
	byt := bmap[q]
	bmap[q] = setbit8(byt, uint8(r))
	return bit / (fbits.cacheline << 3)
}
