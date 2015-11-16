package llrb

import "sort"
import "bytes"
import "hash/crc64"

var crcisotab = crc64.MakeTable(crc64.ISO)

type KVIterator func(key, value []byte) bool

type Dict struct {
	dict map[uint64][2][]byte
}

func NewDict() *Dict {
	return &Dict{dict: make(map[uint64][2][]byte)}
}

func (d *Dict) RSnapshot(_ int) *Dict {
	newd := &Dict{dict: make(map[uint64][2][]byte)}
	for k, v := range d.dict {
		newd.dict[k] = v
	}
	return newd
}

func (d *Dict) Len() int {
	return len(d.dict)
}

func (d *Dict) Has(key []byte) bool {
	hashv := crc64.Checksum(key, crcisotab)
	_, ok := d.dict[hashv]
	return ok
}

func (d *Dict) Get(key []byte) []byte {
	hashv := crc64.Checksum(key, crcisotab)
	if v, ok := d.dict[hashv]; ok {
		return v[1]
	}
	return nil
}

func (d *Dict) Min() (key, value []byte) {
	if len(d.dict) == 0 {
		return nil, nil
	}
	hashv := d.sorted()[0]
	kv := d.dict[uint64(hashv)]
	return kv[0], kv[1]
}

func (d *Dict) Max() (key, value []byte) {
	if len(d.dict) == 0 {
		return nil, nil
	}
	keys := d.sorted()
	kv := d.dict[uint64(keys[len(keys)-1])]
	return kv[0], kv[1]
}

func (d *Dict) DeleteMin() (key, value []byte) {
	if len(d.dict) == 0 {
		return nil, nil
	}
	key, value = d.Min()
	d.Delete(key)
	return key, value
}

func (d *Dict) DeleteMax() (key, value []byte) {
	if len(d.dict) == 0 {
		return nil, nil
	}
	key, value = d.Max()
	d.Delete(key)
	return key, value
}

func (d *Dict) Upsert(key, value []byte) []byte {
	hashv := crc64.Checksum(key, crcisotab)
	oldv := d.dict[hashv]
	d.dict[hashv] = [2][]byte{key, value}
	return oldv[1]
}

func (d *Dict) Delete(key []byte) []byte {
	if len(d.dict) == 0 {
		return nil
	}
	hashv := crc64.Checksum(key, crcisotab)
	oldv := d.dict[hashv]
	delete(d.dict, hashv)
	return oldv[1]
}

func (d *Dict) Range(lowkey, highkey []byte, incl string, iter KVIterator) {
	var start int
	keys := d.sorted()
	cmp := 1
	if incl == "low" || incl == "both" {
		cmp = 0
	}
	for start = 0; start < len(keys); start++ {
		kv := d.dict[uint64(keys[start])]
		if bytes.Compare(lowkey, kv[0]) == cmp {
			break
		}
	}
	if start < len(keys) {
		cmp = -1
		if incl == "high" || incl == "both" {
			cmp = 0
		}
		for i := start; i < len(keys); i++ {
			kv := d.dict[uint64(keys[i])]
			if bytes.Compare(kv[0], highkey) <= cmp {
				if iter(kv[0], kv[1]) == false {
					break
				}
				continue
			}
			break
		}
	}
}

func (d *Dict) sorted() []int {
	keys := make([]int, 0, len(d.dict))
	for key, _ := range d.dict {
		keys = append(keys, int(key))
	}
	sort.Ints(keys)
	return keys
}
