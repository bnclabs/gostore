package bubt

import "io"
import "fmt"

var _ = fmt.Sprintf("")

// Cursor object maintains an active pointer into index. Use OpenCursor
// on Txn object to create a new cursor.
type Cursor struct {
	snap     *Snapshot
	ynext    bool
	shardidx byte
	fposs    []int64

	index    int
	zblock   []byte
	finished bool
}

func (cur *Cursor) opencursor(snap *Snapshot, key []byte) (*Cursor, error) {
	if key == nil { // from beginning
		cur.shardidx = 0
		for i := 0; i < len(snap.readzs); i++ {
			cur.fposs[i] = 0
		}
		if err := cur.nextblock(snap); err != nil {
			return cur, err
		}
		return cur, nil
	}

	shardidx, fpos := snap.findinmblock(key)
	cur.index, _, _, _, _ = snap.findinzblock(shardidx, fpos, key, nil)
	cur.shardidx = shardidx
	for i := byte(0); i < cur.shardidx; i++ {
		cur.fposs[i] = fpos + snap.zblocksize
	}
	for i := cur.shardidx; i < byte(len(snap.readzs)); i++ {
		cur.fposs[i] = fpos
	}
	// populate zblock
	n, err := snap.readzs[shardidx].ReadAt(cur.zblock, fpos)
	if err != nil {
		return nil, err
	} else if n < len(cur.zblock) {
		return nil, fmt.Errorf("bubt.snap.mblock.partialread")
	}
	return cur, nil
}

// Key return key at cursor.
func (cur *Cursor) Key() (key []byte, deleted bool) {
	if cur.finished {
		return nil, false
	} else if cur.index < 0 {
		key, _, _, deleted, _ = cur.getnext()
	} else {
		key, _, _, deleted = zsnap(cur.zblock).entryat(cur.index)
	}
	return
}

// Value return value at cursor.
func (cur *Cursor) Value() (value []byte) {
	if cur.finished {
		return nil
	} else if cur.index < 0 {
		_, value, _, _, _ = cur.getnext()
	} else {
		_, value, _, _ = zsnap(cur.zblock).entryat(cur.index)
	}
	return
}

// GetNext move cursor to next entry and return its key, value, whether
// it is deleted, err will be io.EOF or any other disk error.
func (cur *Cursor) GetNext() (key, value []byte, deleted bool, err error) {
	key, value, _, deleted, err = cur.getnext()
	return
}

func (cur *Cursor) getnext() ([]byte, []byte, uint64, bool, error) {
	if cur.finished {
		return nil, nil, 0, false, io.EOF
	}

	key, value, seqno, deleted := zsnap(cur.zblock).getnext(cur.index)
	if key != nil {
		cur.index++
		return key, value, seqno, deleted, nil
	}
	cur.fposs[cur.shardidx] += cur.snap.zblocksize
	cur.shardidx = (cur.shardidx + 1) % byte(len(cur.fposs))
	if err := cur.nextblock(cur.snap); err != nil {
		return nil, nil, 0, false, err
	}
	key, value, seqno, deleted = zsnap(cur.zblock).entryat(cur.index)
	if key != nil {
		return key, value, seqno, deleted, nil
	}
	cur.finished = true
	return nil, nil, 0, false, io.EOF
}

// YNext can be used for lsm-sort. Similar to GetNext, but includes the
// seqno at which the entry was created/updated/deleted.
func (cur *Cursor) YNext(fin bool) (key,
	value []byte, seqno uint64, deleted bool, err error) {

	if cur.ynext == false {
		cur.ynext = true
		key, value, seqno, deleted = zsnap(cur.zblock).entryat(cur.index)
		return
	}
	return cur.getnext()
}

func (cur *Cursor) nextblock(snap *Snapshot) error {
	for i := 0; i < len(cur.fposs)-1; i++ {
		till := snap.zsizes[cur.shardidx] - snap.zblocksize
		fpos := cur.fposs[cur.shardidx]
		if fpos < till {
			readz := snap.readzs[cur.shardidx]
			n, err := readz.ReadAt(cur.zblock, fpos)
			if err == nil && n == len(cur.zblock) {
				cur.index = 0
				return nil
			}
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
