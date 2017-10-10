package api

import "reflect"
import "unsafe"
import "sync/atomic"

// ParametrisedKey type can be used to encode additional parameters
// into the key without interferring with its sort order. To be
// implemented.
type ParametrisedKey []byte

// pksize return the maximum size required for give key and parameters.
func pksize(key []byte, params keymask) int {
	n := len(key) * 2           // worst case, all ZEROs or all ONEs
	n += 2                      // null-termination
	n = ((n + 8 - 1) >> 3) << 3 // make it 8-byte aligned.
	n += 8 /*hdr*/ + paramsize(params)
	return n
}

// addKeyparameter for a single parameter field.
func addKeyparameter(param keymask, val uint64, values [32]uint64) [32]uint64 {
	values[lookupones[param-1]] = val
	return values
}

// parametriseKey create a new parametric-key with key, params
// of key fields and vbno. If no parameters are used with key, params
// can be ZERO, similarly vbucket number can be ZERO.
func parametriseKey(
	key []byte, params keymask, vbno uint16, values [32]uint64,
	out []byte) ParametrisedKey {

	storeparam := func(out []byte, off int, mask keymask, n int) int {
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
	hdr := keyhdr(0).setmask(params).setvbno(vbno)
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

// parameters return the parameters associated with this key.
func (pk ParametrisedKey) parameters(
	key []byte, params [32]uint64) ([]byte, [32]uint64, uint16, bool) {

	loadparam := func(off int, mask keymask, n int) int {
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

	hdr := keyhdr(atomicload(pk, n))
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

// keyhdr 16-bit flags, 16-bit vbno, 32 bit field selector
// mask(63:32) vbno(32:16) flags[16:0]
type keyhdr uint64

func (kh keyhdr) getmask() keymask {
	return keymask(kh >> 32)
}

func (kh keyhdr) setmask(mask keymask) keyhdr {
	return (kh & 0x00000000FFFFFFFF) | (keyhdr(mask) << 32)
}

func (kh keyhdr) getvbno() uint16 {
	return uint16(keymask(kh>>16) & 0xFFFF)
}

func (kh keyhdr) setvbno(vbno uint16) keyhdr {
	return (kh & 0xFFFFFFFF0000FFFF) | (keyhdr(vbno) << 16)
}

func (kh keyhdr) getflags() keyflags {
	return keyflags(kh & 0xFFFF)
}

func (kh keyhdr) setflags(flags keyflags) keyhdr {
	return (kh & 0xFFFFFFFFFFFF0000) | keyhdr(flags)
}

// keymask 32-bit field mask that can selectively enable or
// disable parameters in key. There can be a maximum of 32 params.
type keymask uint32

const (
	keyParamTxn       keymask = 0x00000001
	keyParamValue     keymask = 0x00000002
	keyParamBornseqno keymask = 0x00000004
	keyParamDeadseqno keymask = 0x00000008
	keyParamUuid      keymask = 0x00000010
)

// enableTxn parameter for key.
func (km keymask) enableTxn() keymask {
	return km | KeyParamTxn
}

// isTxn check Txn parameter for key.
func (km keymask) isTxn() bool {
	return (km & KeyParamTxn) == KeyParamTxn
}

// enableValue parameter for key.
func (km keymask) enableValue() keymask {
	return km | KeyParamValue
}

// isValue check value parameter for key.
func (km keymask) isValue() bool {
	return (km & KeyParamValue) == KeyParamValue
}

// enableBornseqno parameter for key.
func (km keymask) enableBornseqno() keymask {
	return km | KeyParamBornseqno
}

// isBornseqno check bornseqno parameter for key.
func (km keymask) isBornseqno() bool {
	return (km & KeyParamBornseqno) == KeyParamBornseqno
}

// enableDeadseqno parameter for key.
func (km keymask) enableDeadseqno() keymask {
	return km | KeyParamDeadseqno
}

// isDeadseqno check deadseqno parameter for key.
func (km keymask) isDeadseqno() bool {
	return (km & KeyParamDeadseqno) == KeyParamDeadseqno
}

// enableUuid parameter for key.
func (km keymask) enableUuid() keymask {
	return km | KeyParamUuid
}

// isUuid check uuid parameter for key.
func (km keymask) isUuid() bool {
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

func paramsize(mask keymask) int {
	offset := lookupones[mask&0xff]
	mask = mask >> 8
	offset += lookupones[mask&0xff]
	mask = mask >> 8
	offset += lookupones[mask&0xff]
	mask = mask >> 8
	offset += lookupones[mask&0xff]
	return int(offset) * 8
}

// keyflags can be used with ParametrisedKey type.
type keyflags uint16

const (
	keyBlack   keyflags = 0x1
	keyDirty   keyflags = 0x2
	keyDeleted keyflags = 0x4
)

// Setblack flag for llrb node.
func (f keyflags) Setblack() keyflags {
	return f | KeyBlack
}

// Setred flag for llrb node.
func (f keyflags) Setred() keyflags {
	return f & (^(KeyBlack)) // clear the bit
}

// Togglelink toggle red/black flag to black/red flag in llrb node.
func (f keyflags) Togglelink() keyflags {
	return f ^ KeyBlack
}

// Isblack for llrb node.
func (f keyflags) Isblack() bool {
	return (f & KeyBlack) == KeyBlack
}

// Isred for llrb node.
func (f keyflags) Isred() bool {
	return !f.Isblack()
}

// Setdirty set index node dirty.
func (f keyflags) Setdirty() keyflags {
	return f | KeyDirty
}

// Cleardirty clear index node from dirty.
func (f keyflags) Cleardirty() keyflags {
	return f & (^(KeyDirty))
}

// Isdirty check index node is dirty.
func (f keyflags) Isdirty() bool {
	return (f & KeyDirty) == KeyDirty
}

// Setdeleted mark index node as deleted. After marking it as deleted
// there is no going back.
func (f keyflags) Setdeleted() keyflags {
	return f | KeyDeleted
}

// Cleardeleted clear index node from deleted.
func (f keyflags) Cleardeleted() keyflags {
	return f & (^(KeyDeleted))
}

// Isdeleted check whether index node is marked deleted.
func (f keyflags) Isdeleted() bool {
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
