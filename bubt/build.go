package bubt

import "io"
import "fmt"
import "encoding/json"
import "sync/atomic"
import "path/filepath"
import "encoding/binary"

import "github.com/prataprc/gostore/api"
import "github.com/prataprc/golog"
import s "github.com/prataprc/gosettings"

// MarkerBlocksize to close snapshot file.
const MarkerBlocksize = 4096

// MarkerByte to populate Markerblock.
const MarkerByte = 0xAB

// Bubt instance can be used to persist sorted {key,value} entries in
// immutable btree, built bottoms up and not updated there after.
type Bubt struct {
	name      string
	mflusher  *bubtflusher
	zflushers []*bubtflusher

	// settings, will be flushed to the tip of indexfile.
	mblocksize int64
	zblocksize int64
	logprefix  string
}

// NewBubt create a Bubt instance to build a new bottoms-up btree.
func NewBubt(
	name string, paths []string, msize, zsize int64) (tree *Bubt, err error) {

	tree = &Bubt{name: name, mblocksize: msize, zblocksize: zsize}
	mpath, zpaths := tree.pickmzpath(paths)
	tree.logprefix = fmt.Sprintf("BUBT [%s]", name)

	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%v", r)
			tree.Close()
		}
	}()

	mfile := filepath.Join(mpath, name, "bubt-mindex.data")
	if tree.mflusher, err = startflusher(0, mfile); err != nil {
		panic(err)
	}
	tree.zflushers = make([]*bubtflusher, 0)
	for idx, zpath := range zpaths {
		fname := fmt.Sprintf("bubt-zindex-%d.data", idx+1)
		zfile := filepath.Join(zpath, name, fname)
		zflusher, err := startflusher(idx+1, zfile)
		if err != nil {
			panic(err)
		}
		tree.zflushers = append(tree.zflushers, zflusher)
	}
	return tree, nil
}

// Build starts building the tree from iterator, iterator is expected
// to be a full-table scan over another data-store.
func (tree *Bubt) Build(iter api.Iterator, metadata []byte) (err error) {
	log.Infof("%v starting bottoms up build ...\n", tree.logprefix)

	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%v", r)
		}
	}()

	n_count := int64(0)
	z := newz(tree.zblocksize)

	shardidx := 0
	pickzflusher := func() *bubtflusher {
		zflusher := tree.zflushers[shardidx]
		shardidx = (shardidx + 1) % len(tree.zflushers)
		return zflusher
	}

	flushzblock := func(flusher *bubtflusher) int64 {
		if z.finalize() {
			fpos := atomic.LoadInt64(&flusher.fpos)
			if err := flusher.writedata(z.block); err != nil {
				panic(err)
			}
			vpos := int64(flusher.idx<<56) | fpos
			//fmt.Printf("flushzblock %s %x\n", z.firstkey, vpos)
			return vpos
		}
		return -1 // no entries in the block
	}

	flushmblock := func(m *mblock) int64 {
		if m != nil && m.finalize() {
			vpos := atomic.LoadInt64(&tree.mflusher.fpos)
			if err := tree.mflusher.writedata(m.block); err != nil {
				panic(err)
			}
			return vpos
		}
		return -1 // no entries in the block
	}

	var key, value []byte
	var seqno uint64
	var deleted bool

	buildz := func() {
		z.reset()

		if key == nil {
			return
		}
		ok := z.insert(key, value, seqno, deleted)
		if ok == false {
			panic("first insert to zblock, check whether key > zblocksize")
		}
		for ok {
			n_count++
			key, value, seqno, deleted, err = iter(false /*close*/)
			if err != nil && err.Error() != io.EOF.Error() {
				panic(err)
			}
			ok = z.insert(key, value, seqno, deleted)
		}
	}

	var buildm func(m *mblock, level int) (*mblock, *mblock)

	// vpos 8 bit MSB meaning.
	// 0   - points to mblock fpos.
	// 1   - points to zblock's first shard.
	// 255 - points to zblock's 255th shard.
	buildm = func(m *mblock, level int) (m1, m2 *mblock) {
		var vpos int64
		m1 = m

		if m1 == nil {
			return nil, nil

		} else if key == nil { // no more entries
			return m1, nil

		} else if level == 0 { // build leaf node.
			ok := true
			for ok {
				buildz()
				flusher := pickzflusher()
				if vpos = flushzblock(flusher); vpos == -1 {
					return m1, nil
				}
				ok = m1.insert(z.firstkey, vpos)
			}
			m2 = newm(tree.mblocksize)
			m2.insert(z.firstkey, vpos)
			//fmt.Printf("buildm next %s\n", m2.firstkey)
			return m1, m2
		}

		m1, m2 = buildm(m1, level-1)
		if m2 == nil { // done
			return m1, nil
		}
		// m1 can't be nil !!
		var mm *mblock

		m = newm(tree.mblocksize)
		vpos = flushmblock(m1)
		ok := m.insert(m1.firstkey, vpos) // ok is true
		for ok {
			if m1, m2 = buildm(m2, level-1); m1 != nil {
				vpos = flushmblock(m1)
				ok = m.insert(m1.firstkey, vpos)
			} else {
				break
			}
		}
		if ok == false {
			mm = newm(tree.mblocksize)
			mm.insert(m1.firstkey, vpos)
			if m2 != nil {
				mm.insert(m2.firstkey, flushmblock(m2))
			}
		}
		return m, mm
	}

	if iter != nil {
		key, value, seqno, deleted, err = iter(false /*close*/)
		if err != nil && err.Error() != io.EOF.Error() {
			panic(err)

		} else if key != nil {
			m := newm(tree.mblocksize)
			m, _ = buildm(m, 20 /*levels can't go more than 20*/)
			root := flushmblock(m)
			log.Infof("%v root is at %v", tree.logprefix, root)

		} else {
			log.Infof("%v empty iteration", tree.logprefix)
		}
	}

	// flush 1 m-block of settings
	block := make([]byte, MarkerBlocksize)
	setts := s.Settings{
		"name":       tree.name,
		"zblocksize": tree.zblocksize,
		"mblocksize": tree.mblocksize,
		"n_count":    n_count,
	}
	data, _ := json.Marshal(setts)
	if x, y := len(data)+8, len(block); x > y {
		panic(fmt.Errorf("settings(%v) > mblocksize(%v)", x, y))
	}
	binary.BigEndian.PutUint64(block, uint64(len(data)))
	copy(block[8:], data)
	if err := tree.mflusher.writedata(block); err != nil {
		panic(err)
	}
	log.Infof("%v builder wrote settings %v bytes", tree.logprefix, len(block))

	// flush 1 or more m-blocks of metadata
	ln := (((int64(len(metadata)+15) / tree.mblocksize) + 1) * tree.mblocksize)
	block = make([]byte, ln)
	binary.BigEndian.PutUint64(block, uint64(len(metadata)))
	copy(block[8:], metadata)
	binary.BigEndian.PutUint64(block[ln-8:], uint64(ln))
	if err := tree.mflusher.writedata(block); err != nil {
		panic(err)
	}
	log.Infof("%v builder wrote metadata %v bytes", tree.logprefix, len(block))

	log.Infof("%v ... bottoms up build completed\n", tree.logprefix)
	return nil
}

// Close instance after building the btree. This will mark disk files as
// immutable for rest of its life-time. Use OpenSnapshot for reading.
func (tree *Bubt) Close() {
	tree.mflusher.close()
	for _, zflusher := range tree.zflushers {
		zflusher.close()
	}
}

func (tree *Bubt) pickmzpath(paths []string) (string, []string) {
	// TODO: Intelligently pick mpath.
	return paths[0], paths
}
