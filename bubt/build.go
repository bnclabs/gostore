package bubt

import "os"
import "fmt"
import "time"
import "encoding/json"
import "path/filepath"
import "encoding/binary"

import "github.com/prataprc/gostore/api"
import "github.com/prataprc/gostore/lib"
import "github.com/prataprc/golog"
import s "github.com/prataprc/gosettings"

// TODO: Stats for count, depth, n_mblocks, n_zblocks, historgrams.

// Bubt manages sorted {key,value} entries in persisted, immutable btree
// built bottoms up and not updated there after.
type Bubt struct {
	name      string
	mflusher  *bubtflusher
	zflushers []*bubtflusher

	// settings, will be flushed to the tip of indexfile.
	mblocksize int64
	zblocksize int64
	logprefix  string
}

// block level api common to both z-node and m-node.
type blocker interface {
	// start key is the first key in the block, fpos is
	// file-position in index file where key is present.
	startkey() (key []byte)
}

// NewBubt create a Bubt instance to build a new bottoms-up btree.
func NewBubt(name, paths []string, setts s.Settings) *Bubt {
	tree := &Bubt{name: name}.readsettings(setts)
	mpath, zpaths := tree.pickmzpath(paths)
	tree.logprefix = fmt.Sprintf("[BUBT-%s]", name)

	defer func() {
		if r := recover(); r != nil {
			log.Fatalf("%v %v", logprefix, r)
			tree.Close()
		}
	}()

	mfile := filepath.Join(mpath, name, "bubt-mindex.data")
	tree.mflusher = tree.startflusher(0, int(tree.mblocksize), mpath, mfile)
	tree.zflushers = make([]*bubtflusher, 0)
	for idx, zpath := range zpaths {
		fname := fmt.Sprintf("bubt-zindex-%s.data", idx)
		zfile := filepath.Join(zpath, name, fname)
		zflusher := tree.startflusher(idx+1, int(tree.zblocksize), zpath, zfile)
		tree.zflushers = append(tree.zflushers, zflusher)
	}
	return tree
}

func (tree *Bubt) readsettings(setts s.Settings) {
	tree.zblocksize = setts.Int64("zblocksize")
	tree.mblocksize = setts.Int64("mblocksize")
}

// Build starts building the tree from iterator, iterator is expected to be a
// full-table scan over another data-store.
func (tree *Bubt) Build(iter api.Iterator, metadata []byte) {
	log.Infof("%v build started ...\n", tree.logprefix)

	n_count := int64(0)

	rotateidx := 0
	pickzflusher := func() *bubtflusher {
		if len(tree.zflushers) == 0 {
			return mflusher
		}
		zflusher := tree.zflushers[rotateidx]
		rotateidx = (rotateidx + 1) % len(tree.zflushers)
		return zflusher
	}

	flushzblock := func(flusher *bubtflusher) int64 {
		if z.finalize() {
			fpos := atomic.LoadInt64(&flusher.fpos)
			if err := flusher.writedata(z.block); err != nil {
				panic(fmt.Errorf("flushing zblock data: %v", err))
			}
			return fpos
		}
		return -1

	}

	flushmblock := func(m *mblock) int64 {
		if m != nil && m.finalize() {
			fpos := atomic.LoadInt64(&tree.mflusher.fpos)
			if err := tree.mflusher.writedata(m.block); err != nil {
				panic(fmt.Errorf("flushing mblock data: %v", err))
			}
			return fpos
		}
		return -1
	}

	z := newz(tree.zblocksize)
	key, value, seqno, deleted := iter()

	buildz := func() {
		if key == nil {
			return
		} else if ok := z.insert(key, value, seqno, deleted); ok == false {
			panic("first insert to zblock, check whether key > zblocksize")
		}
		for ok {
			n_count++
			key, value, seqno, deleted = iter()
			ok = z.insert(key, value, seqno, deleted)
		}
	}

	// vpos 8 bit MSB meaning.
	// 0   - points to mblock fpos.
	// 1   - points to zblock's first shard.
	// 255 - points to zblock's 255th shard.
	buildm := func(level int) (m *mblock) {
		if key == nil { // no more entries
			return nil

		} else if level == 0 { // build leaf node.
			m = newm(tree.mblocksize)
			ok := true
			for ok {
				buildz()
				flusher := pickzflusher()
				x, vpos := flusher.index, flushzblock(flusher)
				if vpos < 0 {
					return nil
				}
				ok = m.insert(z.firstkey, int64(uint64(vpos)|(x<<56)))
			}
			return m
		}
		m1, m2 := buildm(level-1), buildm(level-1)
		if m2 == nil {
			return m1
		}
		m = newm(tree.mblocksize)
		vpos := flushmblock(m1)
		m.insert(m1.firstkey, int64(vpos))
		vpos = flushmblock(m2)
		ok := m.insert(m2.firstkey, int64(vpos)) // ok should be true
		for ok {
			m2 = buildm(level - 1)
			vpos = flushmblock(m2)
			ok = m.insert(m2.firstkey, int64(vpos))
		}
		return m
	}

	m := buildm(20 /*levels can't go more than 20*/)
	root := flushmblock(m)
	log.Infof("%v root is at %v", tree.logprefix, root)

	// flush 1 m-block of settings
	block := make([]byte, tree.mblocksize)
	setts := s.Settings{
		"name":       tree.name,
		"zblocksize": tree.zblocksize,
		"mblocksize": tree.mblocksize,
		"n_count":    n_count,
	}
	data, _ := json.Marshal(setts)
	if x, y := len(data), len(block); x < y {
		panic(fmt.Errorf("settings(%v) > mblocksize(%v)", x, y))
	}
	copy(block, data)
	if err := tree.mflusher.writedata(block); err != nil {
		panic(fmt.Errorf("writing settings: %v", err))
	}
	log.Infof("%v builder wrote settings %s bytes", tree.logprefix, len(block))

	// flush 1 or more m-blocks of metadata
	ln := ((((len(metadata) + 7) / tree.mblocksize) + 1) * tree.mblocksize)
	block := make([]byte, ln)
	copy(block, metadata)
	binary.BigEndian.PutUint64(block[ln-8:ln], ln)
	if err := tree.mflusher.writedata(block); err != nil {
		panic(fmt.Errorf("writing metadata: %v", err))
	}
	log.Infof("%v builder wrote metadata %s bytes", tree.logprefix, len(block))

	log.Infof("%v ... build completed\n", tree.logprefix)
}

func (tree *Bubt) Close() {
	if tree.mflusher != nil {
		tree.mflusher.close()
	}
	for _, zflusher := range tree.zflushers {
		tree.zflusher.close()
	}
}

func (tree *Bubt) pickmpath(paths []string) (string, []string) {
	if len(paths) == 0 {
		return paths[0], nil
	}
	// TODO: Intelligently pick mpath.
	return paths[0], paths
}
