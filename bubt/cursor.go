package bubt

import "io"
import "fmt"

// Cursor object maintains an active pointer into index. Use OpenCursor
// on Txn object to create a new cursor.
type Cursor struct {
	snap     *Snapshot
	ynext    bool
	shardidx byte
	fposs    []int64

	index    int
	buf      *buffers
	finished bool
}

func (cur *Cursor) opencursor(
	snap *Snapshot, key []byte, buf *buffers) (*Cursor, error) {

	cur.buf = buf
	if key == nil { // from beginning
		cur.shardidx = 0
		for i := 0; i < len(snap.readzs); i++ {
			cur.fposs[i] = 0
		}

		cur.shardidx, cur.index = 0, 0
		// populate zblock
		n, err := snap.readzs[cur.shardidx].ReadAt(cur.buf.zblock, 0)
		if err != nil {
			return nil, err
		} else if n < len(cur.buf.zblock) {
			return nil, fmt.Errorf("bubt.snap.mblock.partialread")
		}
		return cur, nil
	}

	shardidx, fpos := snap.findinmblock(key, buf)
	cur.index, _, _, _, _, _ = snap.findinzblock(shardidx, fpos, key, buf)
	cur.shardidx = shardidx
	for i := byte(0); i < cur.shardidx; i++ {
		cur.fposs[i] = fpos + snap.zblocksize
	}
	for i := cur.shardidx; i < byte(len(snap.readzs)); i++ {
		cur.fposs[i] = fpos
	}
	// populate zblock
	n, err := snap.readzs[shardidx].ReadAt(cur.buf.zblock, fpos)
	if err != nil {
		return nil, err
	} else if n < len(cur.buf.zblock) {
		return nil, fmt.Errorf("bubt.snap.mblock.partialread")
	}
	return cur, nil
}

// Key return key at cursor.
func (cur *Cursor) Key() (key []byte, deleted bool) {
	if cur.finished {
		return nil, false
	}

	z := zsnap(cur.buf.zblock)
	if z.isbounded(cur.index) {
		key, _, _, deleted = z.entryat(cur.index)
	} else {
		key, _, _, deleted, _ = cur.getnext()
	}
	return
}

// Value return value at cursor.
func (cur *Cursor) Value() (value []byte) {
	if cur.finished {
		return nil
	}

	var lv lazyvalue

	z := zsnap(cur.buf.zblock)
	if z.isbounded(cur.index) {
		_, lv, _, _ = z.entryat(cur.index)
		value, cur.buf.vblock = lv.getactual(cur.snap, cur.buf.vblock)

	} else {
		_, lv, _, _, _ = cur.getnext()
		value, cur.buf.vblock = lv.getactual(cur.snap, cur.buf.vblock)
	}
	return
}

// GetNext move cursor to next entry and return its key, value, whether
// it is deleted, err will be io.EOF or any other disk error.
func (cur *Cursor) GetNext() (key, value []byte, deleted bool, err error) {
	var lv lazyvalue

	key, lv, _, deleted, err = cur.getnext()
	value, cur.buf.vblock = lv.getactual(cur.snap, cur.buf.vblock)
	return
}

func (cur *Cursor) getnext() (
	key []byte, lv lazyvalue, seqno uint64, deleted bool, err error) {

	if cur.finished {
		return nil, lv, 0, false, io.EOF
	}

	key, lv, seqno, deleted = zsnap(cur.buf.zblock).getnext(cur.index)
	//fmt.Printf("getnext %q\n", key)
	if key != nil {
		cur.index++
		return key, lv, seqno, deleted, nil
	}

	cur.fposs[cur.shardidx] += cur.snap.zblocksize
	cur.shardidx = (cur.shardidx + 1) % byte(len(cur.fposs))
	err = cur.nextblock(cur.snap)
	if err == nil {
		key, lv, seqno, deleted = zsnap(cur.buf.zblock).entryat(cur.index)
		//fmt.Printf("getnext-next %s\n", key)
		if key != nil {
			return key, lv, seqno, deleted, nil
		}
		panic("impossible situation")
	}
	return nil, lv, 0, false, err
}

// YNext can be used for lsm-sort. Similar to GetNext, but includes the
// seqno at which the entry was created/updated/deleted.
func (cur *Cursor) YNext(fin bool) (key,
	value []byte, seqno uint64, deleted bool, err error) {

	var lv lazyvalue

	cur.finished = fin
	if cur.ynext == false {
		z := zsnap(cur.buf.zblock)
		cur.ynext = true
		if z.isbounded(cur.index) {
			key, lv, seqno, deleted = z.entryat(cur.index)
			value, cur.buf.vblock = lv.getactual(cur.snap, cur.buf.vblock)
			return
		}
	}
	key, lv, seqno, deleted, err = cur.getnext()
	value, cur.buf.vblock = lv.getactual(cur.snap, cur.buf.vblock)
	return
}

func (cur *Cursor) nextblock(snap *Snapshot) error {
	for i := 0; i < len(cur.fposs); i++ {
		till := snap.zsizes[cur.shardidx] - MarkerBlocksize
		fpos := cur.fposs[cur.shardidx]
		if fpos < till {
			readz := snap.readzs[cur.shardidx]
			n, err := readz.ReadAt(cur.buf.zblock, fpos)
			if err != nil {
				errorf("%v %v", cur.snap.logprefix, err)
				return err
			} else if x := len(cur.buf.zblock); n < x {
				err := fmt.Errorf("read %v bytes for zblock %v", n, x)
				errorf("%v %v", cur.snap.logprefix, err)
				return err
			}
			cur.index = 0
			return nil
		}
		// try next shard
		cur.shardidx = (cur.shardidx + 1) % byte(len(cur.fposs))
	}
	cur.finished = true
	return io.EOF
}

// Set not allowed.
func (cur *Cursor) Set(key, value, oldvalue []byte) []byte {
	panic("Set not allowed on view-cursor")
}

// Delete not allowed.
func (cur *Cursor) Delete(key, oldvalue []byte, lsm bool) []byte {
	panic("Delete not allowed on view-cursor")
}

// Delcursor not allowed.
func (cur *Cursor) Delcursor(lsm bool) {
	panic("Delcursor not allowed on view-cursor")
}
