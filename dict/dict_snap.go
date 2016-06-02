package dict

import "strconv"

import "github.com/prataprc/storage.go/api"

// NOTE: sorted() cannot be called on DictSnapshot !!

// DictSnapshot provides a read-only snapshot of Dict map.
type DictSnapshot struct{ Dict }

// NewDictSnapshot create a new instance of DictSnapshot.
func (d *Dict) NewDictSnapshot() api.IndexSnapshot {
	d.snapn++
	snapshot := &DictSnapshot{Dict: Dict{snapn: d.snapn, dead: d.dead}}
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

//---- api.IndexSnapshot{} interface.

// Count implement api.IndexSnapshot{} interface.
func (d *DictSnapshot) Count() int64 {
	return int64(len(d.dict))
}

// ID implement api.IndexSnapshot{} interface.
func (d *DictSnapshot) ID() string {
	return d.id
}

// Isactive implement api.IndexSnapshot{} interface.
func (d *DictSnapshot) Isactive() bool {
	return !d.dead
}

// Refer implement api.IndexSnapshot{} interface.
func (d *DictSnapshot) Refer() {
	return
}

// Release implement api.IndexSnapshot{} interface.
func (d *DictSnapshot) Release() {
	d.dead = true
}

// Validate implement api.IndexSnapshot{} interface.
func (d *DictSnapshot) Validate() {
	panic("Validate(): not implemented for DictSnapshot")
}

//---- IndexReader{} interface.

// Min implement IndexReader{} interface.
func (d *DictSnapshot) Min() api.Node {
	if len(d.dict) == 0 {
		return nil
	}
	return d.dict[d.hashks[0]]
}

// Max implement IndexReader{} interface.
func (d *DictSnapshot) Max() api.Node {
	if len(d.dict) == 0 {
		return nil
	}
	return d.dict[d.hashks[len(d.hashks)-1]]
}

// Range implement IndexReader{} interface.
func (d *DictSnapshot) Range(lk, hk []byte, incl string, reverse bool, iter api.RangeCallb) {
	if reverse {
		d.rangebackward(lk, hk, incl, iter)
		return
	}
	d.rangeforward(lk, hk, incl, iter)
}

// Iterate implement IndexReader{} interface.
func (d *DictSnapshot) Iterate(lkey, hkey []byte, incl string, r bool) api.IndexIterator {
	return d.iterate(lkey, hkey, incl, r)
}

// NOTE: remaining APIS implemented by Dict{}
