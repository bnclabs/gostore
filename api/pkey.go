package api

import "reflect"
import "unsafe"
import "sync/atomic"

// ParametrisedKey type can be used to encode additional parameters
// into the key without interferring with its sort order.
type ParametrisedKey []byte

// PKSize return the maximum size required for give key and parameters.
func PKSize(key []byte, params Keymask) int {
	n := len(key) * 2           // worst case, all ZEROs or all ONEs
	n += 2                      // null-termination
	n = ((n + 8 - 1) >> 3) << 3 // make it 8-byte aligned.
	n += 8 /*hdr*/ + paramsize(params)
	return n
}

// UpdateKeyparameter for a single parameter field.
func AddKeyparameter(param Keymask, val uint64, values [32]uint64) [32]uint64 {
	values[lookupones[param-1]] = val
	return values
}

// ParametriseKey create a new parametric-key with key, params
// of key fields and vbno. If no parameters are used with key, params
// can be ZERO, similarly vbucket number can be ZERO.
func ParametriseKey(
	key []byte, params Keymask, vbno uint16, values [32]uint64,
	out []byte) ParametrisedKey {

	storeparam := func(out []byte, off int, mask Keymask, n int) int {
		for i := off; i < off+8; i++ {
			if (mask & 1) != 0 {
				atomicstore(out, n, values[i])
				n += 8
			}
			mask >>= 1
		}
		return n
	}

	out = keystuff(key, out)
	n := ((len(out) + 8 - 1) >> 3) << 3 // make it 8-byte aligned.
	// provision for hdr and parameters.
	cn := n + 8 /*hdr*/ + paramsize(params)
	out = out[:cn]
	// encode 8-byte hdr
	hdr := Keyhdr(0).setmask(params).setvbno(vbno)
	atomicstore(out, n, uint64(hdr))
	n += 8

	mask, off := params, 0
	if (mask & 0xff) > 0 {
		n += storeparam(out, off, mask, n)
	}
	mask, off = mask>>8, off+8

	if (mask & 0xff) > 0 {
		n += storeparam(out, off, mask, n)
	}
	mask, off = mask>>8, off+8

	if (mask & 0xff) > 0 {
		n += storeparam(out, off, mask, n)
	}
	mask, off = mask>>8, off+8

	if (mask & 0xff) > 0 {
		n += storeparam(out, off, mask, n)
	}
	mask, off = mask>>8, off+8

	return ParametrisedKey(out[:cn])
}

// Parameters return the parameters associated with this key.
func (pk ParametrisedKey) Parameters(
	key []byte, params [32]uint64) ([]byte, [32]uint64, uint16, bool) {

	loadparam := func(off int, mask Keymask, n int) int {
		for i := off; i < off+8; i++ {
			params[i] = 0
			if (mask & 1) != 0 {
				params[i] = atomicload(pk, n)
				n += 8
			}
			mask >>= 1
		}
		return n
	}

	key, n := keyunstuff([]byte(pk), key)
	if n == 0 {
		return key, params, 0, false
	}
	n = ((n + 8 - 1) >> 3) << 3 // make it 8-byte aligned.

	hdr := Keyhdr(atomicload(pk, n))
	vbno, mask := hdr.getvbno(), hdr.getmask()
	n += 8

	off := 0
	if (mask & 0xff) > 0 {
		n += loadparam(off, mask, n)
	}
	mask, off = mask>>8, off+8

	if (mask & 0xff) > 0 {
		n += loadparam(off, mask, n)
	}
	mask, off = mask>>8, off+8

	if (mask & 0xff) > 0 {
		n += loadparam(off, mask, n)
	}
	mask, off = mask>>8, off+8

	if (mask & 0xff) > 0 {
		n += loadparam(off, mask, n)
	}
	mask, off = mask>>8, off+8

	return key, params, vbno, true
}

// Keyhdr 16-bit flags, 16-bit vbno, 32 bit field selector
// mask(63:32) vbno(32:16) flags[16:0]
type Keyhdr uint64

func (kh Keyhdr) getmask() Keymask {
	return Keymask(kh >> 32)
}

func (kh Keyhdr) setmask(mask Keymask) Keyhdr {
	return (kh & 0x00000000FFFFFFFF) | (Keyhdr(mask) << 32)
}

func (kh Keyhdr) getvbno() uint16 {
	return uint16(Keymask(kh>>16) & 0xFFFF)
}

func (kh Keyhdr) setvbno(vbno uint16) Keyhdr {
	return (kh & 0xFFFFFFFF0000FFFF) | (Keyhdr(vbno) << 16)
}

func (kh Keyhdr) getflags() Keyflags {
	return Keyflags(kh & 0xFFFF)
}

func (kh Keyhdr) setflags(flags Keyflags) Keyhdr {
	return (kh & 0xFFFFFFFFFFFF0000) | Keyhdr(flags)
}

// Keymask 32-bit field mask that can selectively enable or
// disable parameters in key. There can be a maximum of 32 params.
type Keymask uint32

const (
	KeyParamTxn       Keymask = 0x00000001
	KeyParamValue     Keymask = 0x00000002
	KeyParamBornseqno Keymask = 0x00000004
	KeyParamDeadseqno Keymask = 0x00000008
	KeyParamUuid      Keymask = 0x00000010
)

// enableTxn parameter for key.
func (km Keymask) enableTxn() Keymask {
	return km | KeyParamTxn
}

// isTxn check Txn parameter for key.
func (km Keymask) isTxn() bool {
	return (km & KeyParamTxn) == KeyParamTxn
}

// enableValue parameter for key.
func (km Keymask) enableValue() Keymask {
	return km | KeyParamValue
}

// isValue check value parameter for key.
func (km Keymask) isValue() bool {
	return (km & KeyParamValue) == KeyParamValue
}

// enableBornseqno parameter for key.
func (km Keymask) enableBornseqno() Keymask {
	return km | KeyParamBornseqno
}

// isBornseqno check bornseqno parameter for key.
func (km Keymask) isBornseqno() bool {
	return (km & KeyParamBornseqno) == KeyParamBornseqno
}

// enableDeadseqno parameter for key.
func (km Keymask) enableDeadseqno() Keymask {
	return km | KeyParamDeadseqno
}

// isDeadseqno check deadseqno parameter for key.
func (km Keymask) isDeadseqno() bool {
	return (km & KeyParamDeadseqno) == KeyParamDeadseqno
}

// enableUuid parameter for key.
func (km Keymask) enableUuid() Keymask {
	return km | KeyParamUuid
}

// isUuid check uuid parameter for key.
func (km Keymask) isUuid() bool {
	return (km & KeyParamUuid) == KeyParamUuid
}

var lookupones = [256]byte{
	0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4,
	1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
	1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
	1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
	3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
	4, 5, 5, 6, 5, 6, 6, 7, 5, 6, 6, 7, 6, 7, 7, 8,
}

func paramsize(mask Keymask) int {
	offset := lookupones[mask&0xff]
	mask = mask >> 8
	offset += lookupones[mask&0xff]
	mask = mask >> 8
	offset += lookupones[mask&0xff]
	mask = mask >> 8
	offset += lookupones[mask&0xff]
	return int(offset) * 8
}

// Keyflags can be used with ParametrisedKey type.
type Keyflags uint16

const (
	KeyBlack   Keyflags = 0x1
	KeyDirty   Keyflags = 0x2
	KeyDeleted Keyflags = 0x4
)

// Setblack flag for llrb node.
func (f Keyflags) Setblack() Keyflags {
	return f | KeyBlack
}

// Setred flag for llrb node.
func (f Keyflags) Setred() Keyflags {
	return f & (^(KeyBlack)) // clear the bit
}

// Togglelink toggle red/black flag to black/red flag in llrb node.
func (f Keyflags) Togglelink() Keyflags {
	return f ^ KeyBlack
}

// Isblack for llrb node.
func (f Keyflags) Isblack() bool {
	return (f & KeyBlack) == KeyBlack
}

// Isred for llrb node.
func (f Keyflags) Isred() bool {
	return !f.Isblack()
}

// Setdirty set index node dirty.
func (f Keyflags) Setdirty() Keyflags {
	return f | KeyDirty
}

// Cleardirty clear index node from dirty.
func (f Keyflags) Cleardirty() Keyflags {
	return f & (^(KeyDirty))
}

// Isdirty check index node is dirty.
func (f Keyflags) Isdirty() bool {
	return (f & KeyDirty) == KeyDirty
}

// Setdeleted mark index node as deleted. After marking it as deleted
// there is no going back.
func (f Keyflags) Setdeleted() Keyflags {
	return f | KeyDeleted
}

// Cleardeleted clear index node from deleted.
func (f Keyflags) Cleardeleted() Keyflags {
	return f & (^(KeyDeleted))
}

// Isdeleted check whether index node is marked deleted.
func (f Keyflags) Isdeleted() bool {
	return (f & KeyDeleted) == KeyDeleted
}

//---- local methods.

func keystuff(in []byte, out []byte) []byte {
	var n int
	for i := 0; i < len(in); i, n = i+1, n+1 {
		switch in[i] {
		case 0x0:
			out[n], n = 0x1, n+1
		case 0x1:
			out[n], n = 0x1, n+1
		}
		out[n] = in[i]
	}
	out[n], out[n+1] = 0x0, 0x0
	return out[:n+2] //  null terminate it.
}

func keyunstuff(in []byte, out []byte) ([]byte, int) {
	var n int
	for i := 0; i < len(in); i, n = i+1, n+1 {
		if in[i] == 0x0 && in[i+1] == 0x0 { // null termination
			return out[:n], i + 2
		} else if in[i] == 0x1 {
			i++
		}
		out[n] = in[i]
	}
	return out, 0
}

// TODO: This can be a problem with Big-endian processors.
func atomicload(buf []byte, off int) uint64 {
	sl := (*reflect.SliceHeader)(unsafe.Pointer(&buf))
	return atomic.LoadUint64((*uint64)(unsafe.Pointer(sl.Data + uintptr(off))))
}

// TODO: This can be a problem with Big-endian processors.
func atomicstore(buf []byte, off int, value uint64) {
	sl := (*reflect.SliceHeader)(unsafe.Pointer(&buf))
	atomic.StoreUint64((*uint64)(unsafe.Pointer(sl.Data+uintptr(off))), value)
}
