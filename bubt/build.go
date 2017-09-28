package bubt

import "fmt"
import "encoding/json"
import "sync/atomic"
import "path/filepath"
import "encoding/binary"

import "github.com/prataprc/gostore/api"
import "github.com/prataprc/golog"
import s "github.com/prataprc/gosettings"

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

// NewBubt create a Bubt instance to build a new bottoms-up btree.
func NewBubt(name string, paths []string, setts s.Settings) *Bubt {
	tree := (&Bubt{name: name}).readsettings(setts)
	mpath, zpaths := tree.pickmzpath(paths)
	tree.logprefix = fmt.Sprintf("[BUBT-%s]", name)

	defer func() {
		if r := recover(); r != nil {
			log.Fatalf("%v %v", tree.logprefix, r)
			tree.Close()
		}
	}()

	mfile := filepath.Join(mpath, name, "bubt-mindex.data")
	tree.mflusher = startflusher(0, int(tree.mblocksize), mfile)
	tree.zflushers = make([]*bubtflusher, 0)
	for idx, zpath := range zpaths {
		fname := fmt.Sprintf("bubt-zindex-%s.data", idx+1)
		zfile := filepath.Join(zpath, name, fname)
		zflusher := startflusher(idx+1, int(tree.zblocksize), zfile)
		tree.zflushers = append(tree.zflushers, zflusher)
	}
	return tree
}

func (tree *Bubt) readsettings(setts s.Settings) *Bubt {
	tree.zblocksize = setts.Int64("zblocksize")
	tree.mblocksize = setts.Int64("mblocksize")
	return tree
}

// Build starts building the tree from iterator, iterator is expected to be a
// full-table scan over another data-store.
func (tree *Bubt) Build(iter api.Iterator, metadata []byte) {
	log.Infof("%v starting bottoms up build ...\n", tree.logprefix)

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
				panic(fmt.Errorf("flushing zblock data: %v", err))
			}
			return int64(flusher.idx<<56) | fpos
		}
		return -1 // no entries in the block
	}

	flushmblock := func(m *mblock) int64 {
		if m != nil && m.finalize() {
			vpos := atomic.LoadInt64(&tree.mflusher.fpos)
			if err := tree.mflusher.writedata(m.block); err != nil {
				panic(fmt.Errorf("flushing mblock data: %v", err))
			}
			return vpos
		}
		return -1 // no entries in the block
	}

	key, value, seqno, deleted := iter()

	buildz := func() {
		if key == nil {
			return
		}
		z.reset()
		ok := z.insert(key, value, seqno, deleted)
		if ok == false {
			panic("first insert to zblock, check whether key > zblocksize")
		}
		for ok {
			n_count++
			key, value, seqno, deleted = iter()
			ok = z.insert(key, value, seqno, deleted)
		}
	}

	var buildm func(level int) *mblock

	// vpos 8 bit MSB meaning.
	// 0   - points to mblock fpos.
	// 1   - points to zblock's first shard.
	// 255 - points to zblock's 255th shard.
	buildm = func(level int) (m *mblock) {
		var vpos int64

		if key == nil { // no more entries
			return nil

		} else if level == 0 { // build leaf node.
			m = newm(tree.mblocksize)
			ok := true
			for ok {
				buildz()
				flusher := pickzflusher()
				if vpos = flushzblock(flusher); vpos == -1 {
					return nil
				}
				ok = m.insert(z.firstkey, vpos)
			}
			return m
		}
		m1, m2 := buildm(level-1), buildm(level-1)
		if m2 == nil {
			return m1
		}
		m = newm(tree.mblocksize)
		m.insert(m1.firstkey, flushmblock(m1))
		ok := m.insert(m2.firstkey, flushmblock(m2)) // ok is true
		for ok {
			m2 = buildm(level - 1)
			ok = m.insert(m2.firstkey, flushmblock(m2))
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
	ln := (((int64(len(metadata)+7) / tree.mblocksize) + 1) * tree.mblocksize)
	block = make([]byte, ln)
	copy(block, metadata)
	binary.BigEndian.PutUint64(block[ln-8:ln], uint64(ln))
	if err := tree.mflusher.writedata(block); err != nil {
		panic(fmt.Errorf("writing metadata: %v", err))
	}
	log.Infof("%v builder wrote metadata %s bytes", tree.logprefix, len(block))

	log.Infof("%v ... bottoms up build completed\n", tree.logprefix)
}

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
