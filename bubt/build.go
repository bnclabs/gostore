package bubt

import "encoding/binary"
import "fmt"

import "github.com/prataprc/storage.go/api"
import "github.com/prataprc/storage.go/log"

// Build starts building the tree from iterator, iterator is expected to be a
// full-table scan over another data-store.
func (f *Bubtstore) Build(iter api.IndexIterator) {
	log.Infof("%v build started ...", f.logprefix)

	f.iterator = iter
	var block blocker

	// add a new level to the btree.
	prependlevel := func(ms []*mblock, mblock *mblock) []*mblock {
		ln := len(ms)
		ms = append(ms, nil)
		copy(ms[1:], ms[:ln])
		ms[0] = mblock
		return ms
	}
	// build
	ms, fpos := []*mblock{}, [2]int64{0, 0}
	for ms, block, fpos = f.buildm(ms, fpos); block != nil; {
		mblock := f.newmblock()
		if mblock.insert(block) == false {
			panic("error inserting first entry into mblock")
		}
		ms, block, fpos = f.buildm(prependlevel(ms, mblock), fpos)
	}

	if len(ms) == 0 {
		log.Infof("%v empty iterator", f.logprefix)
		return
	}

	// root-block and its reduced value.
	block = ms[0]
	f.rootblock, f.rootreduce = block.backref(), block.roffset()

	// flush statistics
	finblock := make([]byte, markerBlocksize)
	if stats := f.stats2json(); len(stats) <= len(finblock) {
		binary.BigEndian.PutUint16(finblock[:2], uint16(len(stats)))
		copy(finblock[2:], stats)
		f.flusher.writeidx(finblock)
	} else {
		panic(fmt.Errorf("stats %v > %v", len(stats), len(finblock)))
	}
	log.Infof("%v wrote stat block\n", f.logprefix)

	// flush configuration
	binary.BigEndian.PutUint64(finblock[:8], uint64(f.rootblock))
	binary.BigEndian.PutUint64(finblock[8:16], uint64(f.rootreduce))
	if config := f.config2json(); len(config) <= len(finblock) {
		binary.BigEndian.PutUint16(finblock[16:18], uint16(len(config)))
		copy(finblock[18:], config)
		f.flusher.writeidx(finblock)
	} else {
		panic(fmt.Errorf("config %v > %v", len(config), len(finblock)))
	}
	log.Infof("%v wrote config block\n", f.logprefix)

	// close and wait for datafile to be sealed.
	f.flusher.close()
	log.Infof("%v ... build completed", f.logprefix)
}

func (f *Bubtstore) buildm(ms []*mblock, fpos [2]int64) ([]*mblock, blocker, [2]int64) {
	var block blocker

	if len(ms) == 0 {
		block, fpos = f.buildz(fpos)
		return ms, block, fpos
	}

	mblock := ms[0]
	f.dcount++
	defer func() { f.dcount-- }()

	ms, block, fpos = f.buildm(ms[1:], fpos)
	for ok := mblock.insert(block); ok; {
		if ms, block, fpos = f.buildm(ms[1:], fpos); block != nil {
			ok = mblock.insert(block)
			continue
		}
		break
	}
	_, fpos = f.flush(mblock, fpos)
	if block != nil {
		ms[0] = f.newmblock()
		if ms[0].insert(block) == false {
			panic("error inserting first entry into mblock")
		}
		return ms, mblock, fpos
	}
	return ms, nil, fpos
}

func (f *Bubtstore) buildz(fpos [2]int64) (blocker, [2]int64) {
	var nd api.Node
	var ok bool

	z := f.newz(fpos)

	f.dcount++
	defer func() { f.dcount-- }()
	defer func() { f.h_depth.Add(f.dcount) }()

	for nd, ok = f.pop(), z.insert(nd); ok; {
		nd = f.pop()
		ok = z.insert(nd)
	}
	if nd != nil {
		f.push(nd)
	}
	return f.flush(z, fpos)
}

func (f *Bubtstore) flush(block blocker, fpos [2]int64) (blocker, [2]int64) {
	switch blk := block.(type) {
	case *zblock:
		if len(blk.entries) > 0 {
			f.a_zentries.Add(int64(len(blk.entries)))
			blk.finalize()
			// reduce
			blk.rpos = fpos[1] + int64(len(blk.dbuffer))
			reducevalue := blk.reduce()
			blk.dbuffer = append(blk.dbuffer, reducevalue...)
			f.a_redsize.Add(int64(len(reducevalue)))
			// move forward [2]fpos
			vpos := fpos[1] + int64(len(blk.dbuffer))
			if err := f.flusher.writedata(blk.dbuffer); err != nil {
				panic(err)
			}
			kpos := fpos[0] + int64(len(blk.kbuffer))
			if int64(len(blk.kbuffer)) > f.zblocksize {
				fmsg := "zblock exceeds size %v"
				panic(fmt.Errorf(fmsg, len(blk.kbuffer), f.zblocksize))
			} else if err := f.flusher.writeidx(blk.kbuffer); err != nil {
				panic(err)
			}
			f.zpool <- blk
			return blk, [2]int64{kpos, vpos}
		}
		return nil, fpos

	case *mblock:
		if len(blk.entries) > 0 {
			f.a_zentries.Add(int64(len(blk.entries)))
			blk.finalize()
			// reduce
			blk.fpos, blk.rpos = fpos, fpos[1]
			reducevalue := blk.reduce()
			f.a_redsize.Add(int64(len(reducevalue)))
			// move forward [2]fpos
			vpos := fpos[1] + int64(len(reducevalue))
			if err := f.flusher.writedata(reducevalue); err != nil {
				panic(err)
			}
			kpos := fpos[0] + int64(len(blk.kbuffer))
			if int64(len(blk.kbuffer)) > f.zblocksize {
				fmsg := "zblock exceeds size %v"
				panic(fmt.Errorf(fmsg, len(blk.kbuffer), f.zblocksize))
			} else if err := f.flusher.writeidx(blk.kbuffer); err != nil {
				panic(err)
			}
			f.mpool <- blk
			return blk, [2]int64{kpos, vpos}
		}
		return nil, fpos
	}
	panic("unreachable code")
}

func (f *Bubtstore) pop() api.Node {
	if ln := len(f.nodes); ln > 0 {
		nd := f.nodes[ln-1]
		f.nodes = f.nodes[:ln-1]
		return nd
	}
	return f.iterator.Next()
}

func (f *Bubtstore) push(nd api.Node) {
	f.nodes = append(f.nodes, nd)
}
