package dict

import "sync/atomic"
import "strconv"

import "github.com/prataprc/gostore/api"

// NOTE: sorted() cannot be called on DictSnapshot !!

// DictSnapshot provides a read-only snapshot of Dict map.
type DictSnapshot struct {
	Dict
	wd    *Dict
	clock api.Clock
}

// NewDictSnapshot create a new instance of DictSnapshot.
func (d *Dict) NewDictSnapshot() api.IndexSnapshot {
	d.snapn++
	snapshot := &DictSnapshot{
		Dict: Dict{
			id: d.id, snapn: d.snapn, dead: atomic.LoadUint32(&d.dead),
		},
		wd:    d,
		clock: d.clock,
	}
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

// Getclock implement api.IndexSnapshot{} interface.
func (d *DictSnapshot) Getclock() api.Clock {
	return d.clock
}

// Refer implement api.IndexSnapshot{} interface.
func (d *DictSnapshot) Refer() {
	return
}

// Isactive implement api.IndexSnapshot{} interface.
func (d *DictSnapshot) Isactive() bool {
	return atomic.LoadUint32(&d.wd.dead) == 0
}

// Release implement api.IndexSnapshot{} interface.
func (d *DictSnapshot) Release() {
	atomic.StoreUint32(&d.dead, 1)
}

// Metadata implement api.IndexSnapshot{} interface.
func (d *DictSnapshot) Metadata() []byte {
	return nil
}

// Validate implement api.IndexSnapshot{} interface.
func (d *DictSnapshot) Validate() {
	panic("Validate(): not implemented for DictSnapshot")
}

//---- IndexReader{} interface.

// Range implement IndexReader{} interface.
func (d *DictSnapshot) Range(lk, hk []byte, incl string, reverse bool, iter api.NodeCallb) {
	lk, hk = d.fixrangeargs(lk, hk)
	if reverse {
		d.rangebackward(lk, hk, incl, iter)
		return
	}
	d.rangeforward(lk, hk, incl, iter)
}

// Iterate implement IndexReader{} interface.
func (d *DictSnapshot) Iterate(lkey, hkey []byte, incl string, r bool) api.IndexIterator {
	lkey, hkey = d.fixrangeargs(lkey, hkey)
	return d.iterate(lkey, hkey, incl, r)
}

// NOTE: remaining APIS implemented by Dict{}
