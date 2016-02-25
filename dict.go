package storage

import "sort"
import "bytes"
import "strconv"
import "hash/crc64"

var crcisotab = crc64.MakeTable(crc64.ISO)

// Dict is a reference data structure, for validation purpose.
type Dict struct {
	id       string
	dict     map[uint64]*dictnode
	sortkeys []string
	hashks   []uint64
	dead     bool
	snapn    int
}

// NewDict create a new golang map for indexing key,value.
func NewDict() *Dict {
	return &Dict{
		id:       "dict",
		dict:     make(map[uint64]*dictnode),
		sortkeys: make([]string, 0, 10000),
	}
}

//---- Index{} interface.

// Count implement Index{} / Snapshot{} interface.
func (d *Dict) Count() int64 {
	return int64(len(d.dict))
}

// Isactive implement Index{} / Snapshot{} interface.
func (d *Dict) Isactive() bool {
	return d.dead == false
}

// RSnapshot implement Index{} interface.
func (d *Dict) RSnapshot() (Snapshot, error) {
	d.snapn++
	newd := NewDict()
	for k, node := range d.dict {
		newd.dict[k] = node
	}
	newd.dead = d.dead
	newd.id = d.id + "-snap" + strconv.Itoa(d.snapn)
	return newd, nil
}

// Destroy implement Index{} interface.
func (d *Dict) Destroy() error {
	d.dead = true
	d.dict, d.sortkeys, d.hashks = nil, nil, nil
	return nil
}

//---- Snapshot{} interface{}

// Id implement Snapshot{} interface.
func (d *Dict) Id() string {
	return d.id
}

// Release implement Snapshot{} interface.
func (d *Dict) Release() {
	d.Destroy()
}

//---- Reader{} interface.

// Has implement Reader{} interface.
func (d *Dict) Has(key []byte) bool {
	hashv := crc64.Checksum(key, crcisotab)
	_, ok := d.dict[hashv]
	return ok
}

// Get implement Reader{} interface.
func (d *Dict) Get(key []byte) Node {
	hashv := crc64.Checksum(key, crcisotab)
	if dn, ok := d.dict[hashv]; ok {
		return dn
	}
	return nil
}

// Min implement Reader{} interface.
func (d *Dict) Min() Node {
	if len(d.dict) == 0 {
		return nil
	}
	hashv := d.sorted()[0]
	dn := d.dict[hashv]
	return dn
}

// Max implement Reader{} interface.
func (d *Dict) Max() Node {
	if len(d.dict) == 0 {
		return nil
	}
	hashks := d.sorted()
	dn := d.dict[hashks[len(hashks)-1]]
	return dn
}

// Range implement Reader{} interface.
func (d *Dict) Range(lowkey, highkey []byte, incl string, iter NodeIterator) {
	var start int
	keys := d.sorted()

	if lowkey == nil {
		start = 0
	} else {
		cmp := 1
		if incl == "low" || incl == "both" {
			cmp = 0
		}
		for start = 0; start < len(keys); start++ {
			dn := d.dict[keys[start]]
			if bytes.Compare(dn.key, lowkey) >= cmp {
				break
			}
		}
	}
	if start < len(keys) {
		cmp := 0
		if incl == "high" || incl == "both" {
			cmp = 1
		}
		for i := start; i < len(keys); i++ {
			dn := d.dict[keys[i]]
			if highkey == nil || (bytes.Compare(dn.key, highkey) < cmp) {
				if iter(dn) == false {
					break
				}
				continue
			}
			break
		}
	}
}

//---- Writer{} interface.

// Upsert implement Writer{} interface.
func (d *Dict) Upsert(key, value []byte, callb UpsertCallback) {
	hashv := crc64.Checksum(key, crcisotab)
	olddn, newdn := d.dict[hashv], newdictnode(key, value)
	d.dict[hashv] = newdn
	callb(d, newdn, olddn)
}

// DeleteMin implement Writer{} interface.
func (d *Dict) DeleteMin(callb DeleteCallback) {
	if len(d.dict) > 0 {
		dn := d.Min()
		d.Delete(dn.Key(), callb)
	}
}

// DeleteMax implement Writer{} interface.
func (d *Dict) DeleteMax(callb DeleteCallback) {
	if len(d.dict) > 0 {
		dn := d.Max()
		d.Delete(dn.Key(), callb)
	}
}

// Delete implement Writer{} interface.
func (d *Dict) Delete(key []byte, callb DeleteCallback) {
	if len(d.dict) > 0 {
		hashv := crc64.Checksum(key, crcisotab)
		deleted := d.dict[hashv]
		callb(d, deleted)
		delete(d.dict, hashv)
	}
}

func (d *Dict) sorted() []uint64 {
	d.sortkeys, d.hashks = d.sortkeys[:0], d.hashks[:0]
	for _, dn := range d.dict {
		d.sortkeys = append(d.sortkeys, bytes2str(dn.key))
	}
	sort.Strings(d.sortkeys)
	for _, key := range d.sortkeys {
		d.hashks = append(d.hashks, crc64.Checksum(str2bytes(key), crcisotab))
	}
	return d.hashks
}
