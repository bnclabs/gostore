package bubt

import "io"
import "fmt"

import "github.com/prataprc/gostore/lib"

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
		cur.shardidx, cur.index = 0, 0
		for i := 0; i < len(snap.readzs); i++ {
			cur.fposs[i] = 0
		}
		cur.zblock = lib.Fixbuffer(cur.zblock, snap.zblocksize)
		readz := snap.readzs[cur.shardidx]
		n, err := readz.ReadAt(cur.zblock, cur.fposs[cur.shardidx])
		if err == io.EOF {
			cur.finished = true
		} else if err != nil {
			return nil, err
		} else if n < len(cur.zblock) {
			return nil, fmt.Errorf("bubt.snap.zblock.partialread")
		}
		return cur, nil
	}

	shardidx, fpos := snap.findinmblock(key)
	cur.index, _, _, _, _ = snap.findinzblock(shardidx, fpos, key)
	cur.shardidx, cur.fposs[shardidx] = shardidx, fpos

	for i := byte(0); i <= cur.shardidx; i++ {
		cur.fposs[i] = fpos
	}
	for i := cur.shardidx + 1; i < byte(len(snap.readzs)); i++ {
		cur.fposs[i] = fpos - snap.zblocksize
	}
	return cur, nil
}

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

func (cur *Cursor) Value() (value []byte) {
	if cur.finished {
		return nil
	} else if cur.index < 0 {
		_, value, _, _, _ = cur.getnext()
	}
	_, value, _, _ = zsnap(cur.zblock).entryat(cur.index)
	return
}

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
	fpos, readz := cur.fposs[cur.shardidx], cur.snap.readzs[cur.shardidx]
	n, err := readz.ReadAt(cur.zblock, fpos)
	if err == nil && n == len(cur.zblock) {
		cur.index = 0
		key, value, seqno, deleted = zsnap(cur.zblock).getnext(cur.index)
		if key != nil {
			cur.index++
			return key, value, seqno, deleted, nil
		}
		panic("impossible situation")
	}
	cur.finished = true
	return nil, nil, 0, false, io.EOF
}

// YNext can be used for lambda-sort or lambda-get.
func (cur *Cursor) YNext() (key, value []byte, seqno uint64, deleted bool, err error) {
	if cur.ynext == false {
		cur.ynext = true
		key, value, seqno, deleted = zsnap(cur.zblock).entryat(cur.index)
		return
	}
	return cur.getnext()
}
