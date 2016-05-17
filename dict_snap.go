// +build dict

package storage

import "strconv"
import "sync/atomic"
import "hash/crc64"
import "bytes"
import "sort"

type DictSnapshot struct {
	id         string
	dict       map[uint64]*dictnode
	sortkeys   []string
	hashks     []uint64
	dead       bool
	snapn      int
	activeiter int64
}

func (d *Dict) NewDictSnapshot() IndexSnapshot {
	d.snapn++
	snapshot := &DictSnapshot{snapn: d.snapn, dead: d.dead}
	snapshot.dict = make(map[uint64]*dictnode)
	for k, node := range d.dict {
		newnode := *node
		snapshot.dict[k] = &newnode
	}
	snapshot.id = d.id + "-snap" + strconv.Itoa(d.snapn)
	snapshot.sorted()
	return snapshot
}

func (d *DictSnapshot) Keys() []string {
	return d.sortkeys
}

//---- IndexSnapshot{} interface.

// Count implement IndexSnapshot{} interface.
func (d *DictSnapshot) Count() int64 {
	return int64(len(d.dict))
}

// Id implement IndexSnapshot{} interface.
func (d *DictSnapshot) Id() string {
	return d.id
}

// Isactive implement IndexSnapshot{} interface.
func (d *DictSnapshot) Isactive() bool {
	return !d.dead
}

// Refer implement IndexSnapshot{} interface.
func (d *DictSnapshot) Refer() {
	return
}

// Release implement IndexSnapshot{} interface.
func (d *DictSnapshot) Release() {
	if atomic.LoadInt64(&d.activeiter) > 0 {
		panic("cannot distroy DictSnapshot when active iterators are present")
	}
	d.dead = true
}

// Validate implement IndexSnapshot{} interface.
func (d *DictSnapshot) Validate() {
	panic("Validate(): not implemented for DictSnapshot")
}

//---- IndexReader{} interface.

// Has implement IndexReader{} interface.
func (d *DictSnapshot) Has(key []byte) bool {
	hashv := crc64.Checksum(key, crcisotab)
	_, ok := d.dict[hashv]
	return ok
}

// Get implement IndexReader{} interface.
func (d *DictSnapshot) Get(key []byte) Node {
	hashv := crc64.Checksum(key, crcisotab)
	if nd, ok := d.dict[hashv]; ok {
		return nd
	}
	return nil
}

// Min implement IndexReader{} interface.
func (d *DictSnapshot) Min() Node {
	if len(d.dict) == 0 {
		return nil
	}
	return d.dict[d.hashks[0]]
}

// Max implement IndexReader{} interface.
func (d *DictSnapshot) Max() Node {
	if len(d.dict) == 0 {
		return nil
	}
	return d.dict[d.hashks[len(d.hashks)-1]]
}

// Range implement IndexReader{} interface.
func (d *DictSnapshot) Range(lk, hk []byte, incl string, reverse bool, iter RangeCallb) {
	if reverse {
		d.rangebackward(lk, hk, incl, iter)
	}
	d.rangeforward(lk, hk, incl, iter)
}

func (d *DictSnapshot) rangeforward(lk, hk []byte, incl string, iter RangeCallb) {
	hashks := d.hashks

	// parameter rewrite for lookup
	if lk != nil && hk != nil && bytes.Compare(lk, hk) == 0 {
		if incl == "none" {
			return
		} else if incl == "low" || incl == "high" {
			incl = "both"
		}
	}
	if len(hashks) == 0 {
		return
	}

	start, cmp, nd := 0, 1, d.dict[hashks[0]]
	if lk != nil {
		if incl == "low" || incl == "both" {
			cmp = 0
		}
		for start = 0; start < len(hashks); start++ {
			nd = d.dict[hashks[start]]
			if bytes.Compare(nd.key, lk) >= cmp {
				break
			}
		}
	}

	cmp = 0
	if incl == "high" || incl == "both" {
		cmp = 1
	}
	for ; start < len(hashks); start++ {
		nd = d.dict[hashks[start]]
		if hk == nil || (bytes.Compare(nd.key, hk) < cmp) {
			if iter(nd) == false {
				break
			}
			continue
		}
		break
	}
}

func (d *DictSnapshot) rangebackward(lk, hk []byte, incl string, iter RangeCallb) {
	hashks := d.sorted()

	// parameter rewrite for lookup
	if lk != nil && hk != nil && bytes.Compare(lk, hk) == 0 {
		if incl == "none" {
			return
		} else if incl == "low" || incl == "high" {
			incl = "both"
		}
	}
	if len(hashks) == 0 {
		return
	}

	start, cmp, nd := len(hashks)-1, -1, d.dict[hashks[0]]
	if hk != nil {
		if incl == "high" || incl == "both" {
			cmp = 0
		}
		for start = len(hashks) - 1; start >= 0; start-- {
			nd = d.dict[hashks[start]]
			if bytes.Compare(nd.key, hk) <= cmp {
				break
			}
		}
	}

	cmp = 0
	if incl == "low" || incl == "both" {
		cmp = -1
	}
	for ; start >= 0; start-- {
		nd = d.dict[hashks[start]]
		if lk == nil || (bytes.Compare(nd.key, lk) > cmp) {
			if iter(nd) == false {
				break
			}
			continue
		}
		break
	}
}

// Iterate implement IndexReader{} interface.
func (d *DictSnapshot) Iterate(lkey, hkey []byte, incl string, r bool) IndexIterator {
	iter := &dictIterator{
		dict: d.dict, hashks: d.sorted(), activeiter: &d.activeiter, reverse: r,
	}

	// parameter rewrite for lookup
	if lkey != nil && hkey != nil && bytes.Compare(lkey, hkey) == 0 {
		if incl == "none" {
			iter.index = len(iter.hashks)
			if r {
				iter.index = -1
			}
			return iter

		} else if incl == "low" || incl == "high" {
			incl = "both"
		}
	}

	startkey, startincl, endincl, cmp := lkey, "low", "high", 1
	iter.endkey, iter.cmp, iter.index = hkey, 0, 0
	if r {
		startkey, startincl, endincl, cmp = hkey, "high", "low", 0
		iter.endkey, iter.cmp, iter.index = lkey, 1, len(iter.hashks)-1
	}

	if startkey != nil {
		if incl == startincl || incl == "both" {
			cmp = 1 - cmp
		}
		for iter.index = 0; iter.index < len(iter.hashks); iter.index++ {
			nd := d.dict[iter.hashks[iter.index]]
			if bytes.Compare(nd.key, startkey) >= cmp {
				break
			}
		}
		if r {
			iter.index--
		}
	}

	if incl == endincl || incl == "both" {
		iter.cmp = 1 - iter.cmp
	}
	atomic.AddInt64(&d.activeiter, 1)
	return iter
}

func (d *DictSnapshot) sorted() []uint64 {
	d.sortkeys, d.hashks = d.sortkeys[:0], d.hashks[:0]
	for _, nd := range d.dict {
		d.sortkeys = append(d.sortkeys, string(nd.key))
	}
	if len(d.sortkeys) > 0 {
		sort.Strings(d.sortkeys)
	}
	for _, key := range d.sortkeys {
		d.hashks = append(d.hashks, crc64.Checksum(str2bytes(key), crcisotab))
	}
	return d.hashks
}
