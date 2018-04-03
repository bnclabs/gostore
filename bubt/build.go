package bubt

import "io"
import "fmt"
import "time"
import "encoding/json"
import "path/filepath"
import "encoding/binary"

import "github.com/bnclabs/gostore/api"
import s "github.com/bnclabs/gosettings"

// MarkerBlocksize to close snapshot file.
const MarkerBlocksize = 4096

// MarkerByte to populate Markerblock.
const MarkerByte = 0xAB

var metadataMarker = []byte("wawaltreatment")

// Bubt instance can be used to persist sorted {key,value} entries in
// immutable btree, built bottoms up and not updated there after.
type Bubt struct {
	name       string
	tombpurge  bool
	mflusher   *bubtflusher
	zflushers  []*bubtflusher
	vflushers  []*bubtflusher
	mdok       bool
	headmblock *mblock

	// settings, will be flushed to the tip of indexfile.
	mblocksize int64
	zblocksize int64
	vblocksize int64
	zeromblock *mblock
	logprefix  string
}

// NewBubt create a Bubt instance to build a new bottoms-up btree.
// If zblocksize == 0, then zblocksize will be same as mblocksize.
// if vblocksize == 0, then values will be stored in value log.
func NewBubt(
	name string, paths []string,
	mblocksize, zblocksize, vblocksize int64) (tree *Bubt, err error) {

	if zblocksize <= 0 {
		zblocksize = mblocksize
	}
	if vblocksize < 0 {
		vblocksize = 0
	}
	tree = &Bubt{
		name:       name,
		mblocksize: mblocksize,
		zblocksize: zblocksize,
		vblocksize: vblocksize,
		tombpurge:  false,
		mdok:       false,
	}
	mpath, zpaths := tree.pickmzpath(paths)
	tree.logprefix = fmt.Sprintf("BUBT [%s]", name)
	tree.zeromblock = newm(tree, tree.mblocksize)

	defer func() {
		if err != nil {
			tree.Close()
		}
	}()

	mfile := filepath.Join(mpath, name, "bubt-mindex.data")
	if tree.mflusher, err = startflusher(0, -1, mfile); err != nil {
		panic(err)
	}
	// if zblocksize <= 0 then zpaths will be empty
	tree.zflushers = tree.makezflushers(zpaths)
	tree.vflushers = tree.makevflushers(zpaths)
	return tree, nil
}

// TombstonePurge to enable or disable purging tombstone entries while
// Building a bubt instance from an iterator.
func (tree *Bubt) TombstonePurge(what bool) {
	tree.tombpurge = what
}

func (tree *Bubt) makezflushers(zpaths []string) []*bubtflusher {
	zflushers := make([]*bubtflusher, 0)
	for idx, zpath := range zpaths {
		// boot zindex files.
		fname := fmt.Sprintf("bubt-zindex-%d.data", idx+1)
		zfile := filepath.Join(zpath, tree.name, fname)
		zflusher, err := startflusher(idx+1, -1, zfile)
		if err != nil {
			panic(err)
		}
		zflushers = append(zflushers, zflusher)
	}
	return zflushers
}

func (tree *Bubt) makevflushers(zpaths []string) []*bubtflusher {
	if tree.vblocksize <= 0 {
		return nil
	}
	vflushers := make([]*bubtflusher, 0)
	for idx, zpath := range zpaths {
		// boot value log files.
		fname := fmt.Sprintf("bubt-vlog-%d.data", idx+1)
		vfile := filepath.Join(zpath, tree.name, fname)
		vflusher, err := startflusher(idx+1, tree.vblocksize, vfile)
		if err != nil {
			panic(err)
		}
		vflushers = append(vflushers, vflusher)
	}
	return vflushers
}

// Build starts building the tree from iterator, iterator is expected
// to be a full-table scan over another data-store.
func (tree *Bubt) Build(iter api.Iterator, metadata []byte) (err error) {
	debugf("%v starting bottoms up build ...\n", tree.logprefix)

	start := time.Now()
	maxseqno, keymem, valmem := uint64(0), uint64(0), uint64(0)
	n_count, n_deleted, paddingmem := int64(0), int64(0), int64(0)
	n_zblocks, n_mblocks, n_vblocks := int64(0), uint64(0), uint64(0)
	compiter := func(
		fin bool) (key, val []byte, seqno uint64, del bool, e error) {

		key, val, seqno, del, e = iter(fin)
		if e == nil {
			// account seqno even for deleted (tombstone) entries.
			if maxseqno < seqno {
				maxseqno = seqno
			}
			if tree.tombpurge && del { // skip accounting for deleted entries
				return key, val, seqno, del, e // wish there is tail recursion
			}
			// account everything else for non-deleted entries.
			keymem = keymem + uint64(len(key))
			if del {
				n_deleted++
			} else {
				valmem += uint64(len(val))
			}
			n_count++
		}
		return key, val, seqno, del, e
	}

	scratchvlog := make([]byte, tree.vblocksize)
	z := newz(tree.zblocksize, tree.vblocksize)

	shardidx := 0
	pickzflusher := func() (zflusher, vflusher *bubtflusher) {
		zflusher = tree.zflushers[shardidx]
		if len(tree.vflushers) > 0 {
			vflusher = tree.vflushers[shardidx]
		}
		shardidx = (shardidx + 1) % len(tree.zflushers)
		return
	}

	flushzblock := func(zflusher *bubtflusher) ([]byte, int64) {
		var vpos int64

		if padded, ok := z.finalize(); ok {
			paddingmem += padded
			fpos := zflusher.fpos

			// accounting and validating
			if ln := len(z.block); ln > 0 {
				if int64(ln) != tree.zblocksize {
					fmsg := "zblock expected %v got %v"
					panic(fmt.Errorf(fmsg, tree.zblocksize, ln))
				}
				n_zblocks++
			}

			if err := zflusher.writedata(z.block); err != nil {
				panic(err)
			}
			vpos = int64(zflusher.idx<<56) | fpos
			//fmt.Printf("flushzblock %s %x\n", z.firstkey, vpos)
			return z.vlog, vpos
		}
		return z.vlog, -1 // no entries in the block
	}

	flushvblock := func(vflusher *bubtflusher) {
		if vflusher == nil { // value is in zblocks.
			return
		}
		vlog := vflusher.vlog
		// take till vblocksize boundary and return the remaining.
		till := (int64(len(vlog)) / tree.vblocksize) * tree.vblocksize
		remn := int64(len(vlog)) % tree.vblocksize
		//fmt.Println(till, remn, len(vlog), cap(vlog))
		block := vlog[:till]

		// accounting and validating
		if ln := int64(len(block)); ln > 0 {
			if (ln % tree.vblocksize) != 0 {
				fmsg := "vblock %v expected in multiples of %v"
				panic(fmt.Errorf(fmsg, ln, tree.vblocksize))
			}
			n_vblocks += uint64(ln / tree.vblocksize)
		}

		if err := vflusher.writedata(vlog[:till]); err != nil {
			panic(err)
		}
		copy(scratchvlog[:remn], vlog[till:till+remn])
		vflusher.vlog = append(vlog[:0], scratchvlog[:remn]...)
		//fmt.Println(len(vflusher.vlog), cap(vflusher.vlog))
		return
	}

	flushmblock := func(m *mblock) int64 {
		if m == nil {
			return -1 // no entries
		}
		if padded, ok := m.finalize(); ok {
			paddingmem += padded
			vpos := tree.mflusher.fpos

			// accounting and validating
			if ln := int64(len(m.block)); ln > 0 {
				if ln != tree.mblocksize {
					fmsg := "mblock expected %v, got %v"
					panic(fmt.Errorf(fmsg, tree.vblocksize, ln))
				}
				n_mblocks++
			}

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
		if key == nil {
			return
		}

		ok := true
		if (tree.tombpurge && deleted == false) || tree.tombpurge == false {
			ok = z.insert(key, value, seqno, deleted)
			if ok == false {
				panic("first insert to zblock, check whether key > zblocksize")
			}
		}
		for ok {
			key, value, seqno, deleted, err = compiter(false /*close*/)
			if err == io.EOF {
				break
			} else if err != nil {
				panic(err)
			}
			if (tree.tombpurge && deleted == false) || tree.tombpurge == false {
				ok = z.insert(key, value, seqno, deleted)
			}
		}
		return
	}

	var buildm func(m1 *mblock, level int) (*mblock, *mblock)

	// vpos 8 bit MSB meaning.
	// 0   - points to mblock fpos.
	// 1   - points to zblock's first shard.
	// 255 - points to zblock's 255th shard.
	buildm = func(m1 *mblock, level int) (*mblock, *mblock) {
		if m1 == nil {
			return nil, nil

		} else if key == nil { // no more entries
			return m1, nil

		} else if level == 0 { // build leaf node.
			var vpos int64
			ok := true
			for ok {
				zflusher, vflusher := pickzflusher()
				vlogpos, vlog := int64(0), []byte(nil)
				if vflusher != nil {
					vlogpos, vlog = vflusher.fpos, vflusher.vlog
					vlogpos += int64(len(vlog))
				}
				z.reset(vlogpos, vlog)

				buildz()

				vlog, vpos = flushzblock(zflusher)
				if vflusher != nil {
					vflusher.vlog = vlog
				}
				if vpos == -1 {
					return m1, nil
				}
				flushvblock(vflusher)
				ok = m1.insert(z.firstkey, vpos)
			}
			m2 := newm(tree, tree.mblocksize)
			m2.insert(z.firstkey, vpos)
			//fmt.Printf("buildm next %s\n", m2.firstkey)
			return m1, m2
		}

		m1, m2 := buildm(m1, level-1)
		if m2 == nil { // done
			return m1, nil
		}
		// m1 can't be nil !!
		var mm *mblock

		m := newm(tree, tree.mblocksize)
		vpos := flushmblock(m1)
		ok := m.insert(m1.firstkey, vpos) // ok is true
		for ok {
			putm(tree, m1)
			if m1, m2 = buildm(m2, level-1); m1 != nil {
				vpos = flushmblock(m1)
				ok = m.insert(m1.firstkey, vpos)
			} else {
				break
			}
		}
		if ok == false {
			mm = newm(tree, tree.mblocksize)
			mm.insert(m1.firstkey, vpos)
			if m2 != nil {
				mm.insert(m2.firstkey, flushmblock(m2))
			}
		}
		return m, mm
	}

	// start building the tree, with maximum fill possible rate.
	var root int64
	if iter != nil {
		key, value, seqno, deleted, err = compiter(false /*close*/)
		if err != nil && err.Error() != io.EOF.Error() {
			panic(err)

		} else if err == nil && key != nil {
			m := newm(tree, tree.mblocksize)
			m, _ = buildm(m, 20 /*levels can't go more than 20*/)
			root = flushmblock(m)

		} else {
			infof("%v empty iteration", tree.logprefix)
		}
	}

	// flush away partial value logs
	flushvlog := make([]byte, tree.vblocksize)
	for _, vflusher := range tree.vflushers {
		if len(vflusher.vlog) == 0 {
			continue
		}
		if int64(len(vflusher.vlog)) > tree.vblocksize {
			fmsg := "partial value log %v cannot be more than %v"
			panic(fmt.Errorf(fmsg, len(vflusher.vlog), tree.vblocksize))
		}
		copy(flushvlog, vflusher.vlog)
		if err := vflusher.writedata(flushvlog); err != nil {
			panic(err)
		}
		paddingmem += (tree.vblocksize - int64(len(vflusher.vlog)))
		n_vblocks++
		for i := range flushvlog {
			flushvlog[i] = 0
		}
		vflusher.vlog = vflusher.vlog[:0]
	}

	// flush 1 MarkerBlocksize of infoblock
	block := make([]byte, MarkerBlocksize)
	infoblock := s.Settings{
		"name":       tree.name,
		"zblocksize": tree.zblocksize,
		"mblocksize": tree.mblocksize,
		"vblocksize": tree.vblocksize,
		"buildtime":  fmt.Sprintf("%d", time.Since(start)),
		"epoch":      fmt.Sprintf("%d", time.Now().Unix()),
		"seqno":      fmt.Sprintf("%d", maxseqno),
		"keymem":     fmt.Sprintf("%d", keymem),
		"valmem":     fmt.Sprintf("%d", valmem),
		"paddingmem": fmt.Sprintf("%d", paddingmem),
		"n_zblocks":  fmt.Sprintf("%d", n_zblocks),
		"n_mblocks":  fmt.Sprintf("%d", n_mblocks),
		"n_vblocks":  fmt.Sprintf("%d", n_vblocks),
		"n_count":    fmt.Sprintf("%d", n_count),
		"n_deleted":  fmt.Sprintf("%d", n_deleted),
	}
	data, _ := json.Marshal(infoblock)
	if x, y := len(data)+8, len(block); x > y {
		panic(fmt.Errorf("infoblock(%v) > MarkerBlocksize", x, y))
	}
	binary.BigEndian.PutUint64(block, uint64(len(data)))
	copy(block[8:], data)
	if err := tree.mflusher.writedata(block); err != nil {
		panic(err)
	}
	infoblkn := len(block)

	// flush 1 or more m-blocks of metadata
	var lenMetadata int
	if len(metadata) > 0 {
		lenMetadata, err = tree.Writemetadata(metadata)
		if err != nil {
			return err
		}
		tree.mdok = true
	}

	fmsg := "%v built with root@%v %v bytes infoblock %v bytes metadata"
	infof(fmsg, tree.logprefix, root, infoblkn, lenMetadata)
	return nil
}

func (tree *Bubt) Writemetadata(metadata []byte) (int, error) {
	ln := (((int64(len(metadata)+15) / tree.mblocksize) + 1) * tree.mblocksize)
	block := make([]byte, ln)
	binary.BigEndian.PutUint64(block, uint64(len(metadata)))
	copy(block[8:], metadata)
	binary.BigEndian.PutUint64(block[ln-8:], uint64(ln))
	if err := tree.mflusher.writedata(block); err != nil {
		panic(err)
	}
	infof("%v wrote %v bytes metadata", tree.logprefix, len(metadata))
	tree.mdok = true
	return len(block), nil
}

// Close instance after building the btree. This will mark disk files as
// immutable for rest of its life-time. Use OpenSnapshot for reading.
func (tree *Bubt) Close() {
	// if metadata is not flushed, flush an empty metadata.
	if tree.mdok == false {
		tree.Writemetadata(metadataMarker)
		tree.mdok = true
	}

	if tree.mflusher != nil {
		tree.mflusher.close()
	}
	for _, zflusher := range tree.zflushers {
		zflusher.close()
	}
	for _, vflusher := range tree.vflushers {
		vflusher.close()
	}
}

func (tree *Bubt) pickmzpath(paths []string) (string, []string) {
	// TODO: Intelligently pick mpath.
	mpath, zpaths := paths[0], []string{}
	if tree.zblocksize > 0 {
		zpaths = append(zpaths, paths...)
		return mpath, zpaths
	}
	return mpath, zpaths
}
