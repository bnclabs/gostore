// +build dict

package storage

import "strconv"
import "sync/atomic"

// NOTE: sorted() cannot be called on DictSnapshot !!

type DictSnapshot struct{ Dict }

func (d *Dict) NewDictSnapshot() IndexSnapshot {
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
		return
	}
	d.rangeforward(lk, hk, incl, iter)
}

// Iterate implement IndexReader{} interface.
func (d *DictSnapshot) Iterate(lkey, hkey []byte, incl string, r bool) IndexIterator {
	return d.iterate(lkey, hkey, incl, r)
}

// NOTE: remaining APIS implemented by Dict{}
