package storage

import "strconv"
import "hash/crc64"
import "bytes"
import "sort"

type DictSnapshot struct {
	id       string
	dict     map[uint64]*dictnode
	sortkeys []string
	hashks   []uint64
	dead     bool
	snapn    int
}

func (d *Dict) NewDictSnapshot() Snapshot {
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

//---- Snapshot{} interface.

// Count implement Snapshot{} interface.
func (d *DictSnapshot) Count() int64 {
	return int64(len(d.dict))
}

// Id implement Snapshot{} interface.
func (d *DictSnapshot) Id() string {
	return d.id
}

// Isactive implement Snapshot{} interface.
func (d *DictSnapshot) Isactive() bool {
	return !d.dead
}

// Refer implement Snapshot{} interface.
func (d *DictSnapshot) Refer() {
	return
}

// Release implement Snapshot{} interface.
func (d *DictSnapshot) Release() {
	d.dead = true
}

// Validate implement Snapshot{} interface.
func (d *DictSnapshot) Validate() {
	panic("Validate(): not implemented for DictSnapshot")
}

//---- Reader{} interface.

// Has implement Reader{} interface.
func (d *DictSnapshot) Has(key []byte) bool {
	hashv := crc64.Checksum(key, crcisotab)
	_, ok := d.dict[hashv]
	return ok
}

// Get implement Reader{} interface.
func (d *DictSnapshot) Get(key []byte) Node {
	hashv := crc64.Checksum(key, crcisotab)
	if nd, ok := d.dict[hashv]; ok {
		return nd
	}
	return nil
}

// Min implement Reader{} interface.
func (d *DictSnapshot) Min() Node {
	if len(d.dict) == 0 {
		return nil
	}
	return d.dict[d.hashks[0]]
}

// Max implement Reader{} interface.
func (d *DictSnapshot) Max() Node {
	if len(d.dict) == 0 {
		return nil
	}
	return d.dict[d.hashks[len(d.hashks)-1]]
}

// Range implement Reader{} interface.
func (d *DictSnapshot) Range(lowkey, highkey []byte, incl string, iter NodeIterator) {
	var start int
	var hashks []uint64

	hashks = d.hashks

	if lowkey == nil {
		start = 0
	} else {
		cmp := 1
		if incl == "low" || incl == "both" {
			cmp = 0
		}
		for start = 0; start < len(hashks); start++ {
			nd := d.dict[hashks[start]]
			if bytes.Compare(nd.key, lowkey) >= cmp {
				break
			}
		}
	}
	if start < len(hashks) {
		cmp := 0
		if incl == "high" || incl == "both" {
			cmp = 1
		}
		for i := start; i < len(hashks); i++ {
			nd := d.dict[hashks[i]]
			if highkey == nil || (bytes.Compare(nd.key, highkey) < cmp) {
				if iter(nd) == false {
					break
				}
				continue
			}
			break
		}
	}
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
