package storage

import "sort"
import "bytes"
import "hash/crc64"

var crcisotab = crc64.MakeTable(crc64.ISO)

type KVIterator func(key, value []byte) bool

type Dict struct {
	dict     map[uint64][2][]byte
	sortkeys []string
	hashks   []uint64
}

func NewDict() *Dict {
	return &Dict{
		dict:     make(map[uint64][2][]byte),
		sortkeys: make([]string, 0, 10000),
	}
}

func (d *Dict) RSnapshot() *Dict {
	newd := NewDict()
	for k, v := range d.dict {
		newd.dict[k] = v
	}
	return newd
}

func (d *Dict) Count() int64 {
	return int64(len(d.dict))
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
	kv := d.dict[hashv]
	return kv[0], kv[1]
}

func (d *Dict) Max() (key, value []byte) {
	if len(d.dict) == 0 {
		return nil, nil
	}
	hashks := d.sorted()
	kv := d.dict[hashks[len(hashks)-1]]
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

	if lowkey == nil {
		start = 0
	} else {
		cmp := 1
		if incl == "low" || incl == "both" {
			cmp = 0
		}
		for start = 0; start < len(keys); start++ {
			kv := d.dict[keys[start]]
			if bytes.Compare(kv[0], lowkey) >= cmp {
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
			kv := d.dict[keys[i]]
			if highkey == nil || (bytes.Compare(kv[0], highkey) < cmp) {
				if iter(kv[0], kv[1]) == false {
					break
				}
				continue
			}
			break
		}
	}
}

func (d *Dict) sorted() []uint64 {
	d.sortkeys, d.hashks = d.sortkeys[:0], d.hashks[:0]
	for _, kv := range d.dict {
		d.sortkeys = append(d.sortkeys, bytes2str(kv[0]))
	}
	sort.Strings(d.sortkeys)
	for _, key := range d.sortkeys {
		d.hashks = append(d.hashks, crc64.Checksum(str2bytes(key), crcisotab))
	}
	return d.hashks
}
