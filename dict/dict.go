// Package dict implement a dictionary of key,value pairs based on golang
// map. Primarily meant as reference for testing more useful storage
// algorithms.
package dict

import "sort"
import "bytes"
import "fmt"
import "sync/atomic"
import "hash/crc64"

import "github.com/prataprc/storage.go/api"
import "github.com/prataprc/storage.go/lib"

var _ = fmt.Sprintf("dummy")

var crcisotab = crc64.MakeTable(crc64.ISO)

// Dict is a reference data structure, for validation purpose.
type Dict struct {
	id         string
	dict       map[uint64]*dictnode
	sortkeys   []string
	hashks     []uint64
	dead       bool
	snapn      int
	activeiter int64
}

// NewDict create a new golang map for indexing key,value.
func NewDict() *Dict {
	return &Dict{
		id:       "dict",
		dict:     make(map[uint64]*dictnode),
		sortkeys: make([]string, 0, 10000),
	}
}

//---- api.Index{} interface.

// Count implement api.Index{} / api.IndexSnapshot{} interface.
func (d *Dict) Count() int64 {
	return int64(len(d.dict))
}

// Isactive implement api.Index{} / api.IndexSnapshot{} interface.
func (d *Dict) Isactive() bool {
	return d.dead == false
}

// RSnapshot implement api.Index{} interface.
func (d *Dict) RSnapshot(snapch chan api.IndexSnapshot) error {
	snapch <- d.NewDictSnapshot()
	return nil
}

// Destroy implement api.Index{} interface.
func (d *Dict) Destroy() error {
	if atomic.LoadInt64(&d.activeiter) > 0 {
		panic("cannot distroy Dict when iterators are active")
	}

	d.dead = true
	d.dict, d.sortkeys, d.hashks = nil, nil, nil
	return nil
}

// Stats implement api.Index{} interface.
func (d *Dict) Stats() (map[string]interface{}, error) {
	panic("dict.Stats() not implemented for Dict")
}

// Fullstats implement api.Index{} interface.
func (d *Dict) Fullstats() (map[string]interface{}, error) {
	panic("dict.Fullstats() not implemented for Dict")
}

// Validate implement api.Index{} interface.
func (d *Dict) Validate() {
	panic("dict.Validate() not implemented for Dict")
}

// Log implement api.Index{} interface.
func (d *Dict) Log(involved int, humanize bool) {
	panic("dict.Log() not implemented for Dict")
}

//---- api.IndexSnapshot{} interface{}

// ID implement api.IndexSnapshot{} interface.
func (d *Dict) ID() string {
	return d.id
}

// Refer implement api.IndexSnapshot{} interface.
func (d *Dict) Refer() {
	return
}

// Release implement api.IndexSnapshot{} interface.
func (d *Dict) Release() {
	d.Destroy()
}

//---- IndexReader{} interface.

// Has implement IndexReader{} interface.
func (d *Dict) Has(key []byte) bool {
	hashv := crc64.Checksum(key, crcisotab)
	_, ok := d.dict[hashv]
	return ok
}

// Get implement IndexReader{} interface.
func (d *Dict) Get(key []byte, callb api.NodeCallb) bool {
	hashv := crc64.Checksum(key, crcisotab)
	if nd, ok := d.dict[hashv]; ok {
		if callb == nil {
			return true
		}
		return callb(nd)
	}
	return false
}

// Min implement IndexReader{} interface.
func (d *Dict) Min(callb api.NodeCallb) bool {
	if len(d.dict) == 0 {
		return false
	}
	hashv := d.sorted()[0]
	if callb == nil {
		return true
	}
	return callb(d.dict[hashv])
}

// Max implement IndexReader{} interface.
func (d *Dict) Max(callb api.NodeCallb) bool {
	if len(d.dict) == 0 {
		return false
	}
	hashks := d.sorted()
	if callb == nil {
		return true
	}
	return callb(d.dict[hashks[len(hashks)-1]])
}

// Range implement IndexReader{} interface.
func (d *Dict) Range(lk, hk []byte, incl string, reverse bool, iter api.RangeCallb) {
	lk, hk = d.fixrangeargs(lk, hk)
	d.sorted()
	if reverse {
		d.rangebackward(lk, hk, incl, iter)
		return
	}
	d.rangeforward(lk, hk, incl, iter)
}

func (d *Dict) rangeforward(lk, hk []byte, incl string, iter api.RangeCallb) {
	hashks := d.hashks
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
			if api.Binarycmp(nd.key, lk, cmp == 1) >= cmp {
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
		if hk == nil || (api.Binarycmp(nd.key, hk, cmp == 1) < cmp) {
			if iter(nd) == false {
				break
			}
			continue
		}
		break
	}
}

func (d *Dict) rangebackward(lk, hk []byte, incl string, iter api.RangeCallb) {
	hashks := d.hashks
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
			if api.Binarycmp(nd.key, hk, cmp == 0) <= cmp {
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
		if lk == nil || (api.Binarycmp(nd.key, lk, cmp == 0) > cmp) {
			if iter(nd) == false {
				break
			}
			continue
		}
		break
	}
}

// Iterate implement IndexReader{} interface.
func (d *Dict) Iterate(lkey, hkey []byte, incl string, r bool) api.IndexIterator {
	lkey, hkey = d.fixrangeargs(lkey, hkey)
	d.sorted()
	return d.iterate(lkey, hkey, incl, r)
}

func (d *Dict) iterate(lkey, hkey []byte, incl string, r bool) api.IndexIterator {
	if lkey != nil && hkey != nil && bytes.Compare(lkey, hkey) == 0 {
		if incl == "none" {
			return nil
		} else if incl == "low" || incl == "high" {
			incl = "both"
		}
	}

	iter := &iterator{}

	// NOTE: always re-initialize, because we are getting it back from pool.
	iter.tree, iter.dict = d, d
	iter.nodes, iter.index, iter.limit = iter.nodes[:0], 0, 5
	iter.continuate = false
	iter.startkey, iter.endkey, iter.incl, iter.reverse = lkey, hkey, incl, r
	iter.closed, iter.activeiter = false, &d.activeiter

	if iter.nodes == nil {
		iter.nodes = make([]api.Node, 0)
	}

	iter.rangefill()
	if r {
		switch iter.incl {
		case "none":
			iter.incl = "high"
		case "low":
			iter.incl = "both"
		}
	} else {
		switch iter.incl {
		case "none":
			iter.incl = "low"
		case "high":
			iter.incl = "both"
		}
	}

	atomic.AddInt64(&d.activeiter, 1)
	return iter
}

//---- IndexWriter{} interface.

// Upsert implement IndexWriter{} interface.
func (d *Dict) Upsert(key, value []byte, callb api.UpsertCallback) error {
	newnd := newdictnode(key, value)
	hashv := crc64.Checksum(key, crcisotab)
	oldnd, ok := d.dict[hashv]
	if callb != nil {
		if ok == false {
			callb(d, 0, newnd, nil)
		} else {
			callb(d, 0, newnd, oldnd)
		}
	}
	d.dict[hashv] = newnd
	return nil
}

// UpsertMany implement IndexWriter{} interface.
func (d *Dict) UpsertMany(keys, values [][]byte, callb api.UpsertCallback) error {
	for i, key := range keys {
		var value []byte
		if len(values) > 0 {
			value = values[i]
		}
		newnd := newdictnode(key, value)
		hashv := crc64.Checksum(key, crcisotab)
		oldnd, ok := d.dict[hashv]
		if callb != nil {
			if ok == false {
				callb(d, int64(i), newnd, nil)
			} else {
				callb(d, int64(i), newnd, oldnd)
			}
		}
		d.dict[hashv] = newnd
	}
	return nil
}

// DeleteMin implement IndexWriter{} interface.
func (d *Dict) DeleteMin(callb api.DeleteCallback) error {
	if len(d.dict) > 0 {
		d.Min(func(nd api.Node) bool {
			d.Delete(nd.Key(), callb)
			return true
		})
	}
	return nil
}

// DeleteMax implement IndexWriter{} interface.
func (d *Dict) DeleteMax(callb api.DeleteCallback) error {
	if len(d.dict) > 0 {
		d.Max(func(nd api.Node) bool {
			d.Delete(nd.Key(), callb)
			return true
		})
	}
	return nil
}

// Delete implement IndexWriter{} interface.
func (d *Dict) Delete(key []byte, callb api.DeleteCallback) error {
	if len(d.dict) > 0 {
		hashv := crc64.Checksum(key, crcisotab)
		deleted, ok := d.dict[hashv]
		if callb != nil {
			if ok == false {
				callb(d, nil)
			} else {
				callb(d, deleted)
			}
		}
		delete(d.dict, hashv)
	}
	return nil
}

func (d *Dict) sorted() []uint64 {
	d.sortkeys, d.hashks = d.sortkeys[:0], d.hashks[:0]
	for _, nd := range d.dict {
		d.sortkeys = append(d.sortkeys, string(nd.key))
	}
	if len(d.sortkeys) > 0 {
		sort.Strings(d.sortkeys)
	}
	for _, key := range d.sortkeys {
		hashk := crc64.Checksum(lib.Str2bytes(key), crcisotab)
		d.hashks = append(d.hashks, hashk)
	}
	return d.hashks
}

func (d *Dict) fixrangeargs(lk, hk []byte) ([]byte, []byte) {
	l, h := lk, hk
	if len(lk) == 0 {
		l = nil
	}
	if len(hk) == 0 {
		h = nil
	}
	return l, h
}
