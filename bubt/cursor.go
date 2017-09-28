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

func (cur *Cursor) opencursor(snap *Snapshot, key []byte) *Cursor {
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
			panic(err)
		} else if n < len(cur.zblock) {
			panic(fmt.Errorf("zblock read only %v(%v)", n, cur.zblock))
		}
		return cur
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
	return cur
}

func (cur *Cursor) Key() (key []byte, deleted bool) {
	if cur.finished {
		return nil, false
	}
	key, _, _, deleted = zsnap(cur.zblock).entryat(cur.index)
	return
}

func (cur *Cursor) Value() (value []byte) {
	if cur.finished {
		return nil
	}
	_, value, _, _ = zsnap(cur.zblock).entryat(cur.index)
	return
}

func (cur *Cursor) GetNext() (key, value []byte, deleted bool) {
	if cur.finished {
		return nil, nil, false
	}
	key, value, _, deleted = cur.getnext()
	return
}

func (cur *Cursor) getnext() (key, value []byte, seqno uint64, deleted bool) {
	key, value, seqno, deleted = zsnap(cur.zblock).getnext(cur.index)
	if key != nil {
		cur.index++
		return
	}
	cur.fposs[cur.shardidx] += cur.snap.zblocksize

	for i := 0; i < len(cur.fposs)-1; i++ {
		cur.shardidx = (cur.shardidx + 1) % byte(len(cur.fposs))
		fpos, readz := cur.fposs[cur.shardidx], cur.snap.readzs[cur.shardidx]
		n, err := readz.ReadAt(cur.zblock, fpos)
		if err == io.EOF {
			continue
		} else if err != nil {
			panic(err)
		} else if n < len(cur.zblock) {
			panic(fmt.Errorf("zblock read only %v(%v)", n, cur.zblock))
		}
		cur.index = 0
		key, value, seqno, deleted = zsnap(cur.zblock).getnext(cur.index)
		if key != nil {
			cur.index++
			return
		}
	}
	cur.finished = true
	return nil, nil, 0, false
}

// YNext can be used for lambda-sort or lambda-get.
func (cur *Cursor) YNext() (key, value []byte, seqno uint64, deleted bool) {
	if cur.finished {
		return nil, nil, 0, false
	} else if cur.ynext == false {
		cur.ynext = true
		key, value, seqno, deleted = zsnap(cur.zblock).entryat(cur.index)
		return
	}
	return cur.getnext()
}
