package bubt

import "encoding/binary"
import "encoding/json"
import "fmt"
import "os"

import "github.com/prataprc/storage.go/api"
import "github.com/prataprc/storage.go/lib"
import "github.com/prataprc/storage.go/log"

// Bubt manages sorted {key,value} entries in persisted, immutable btree
// built bottoms up and not updated there after.
type Bubt struct {
	rootblock  int64
	rootreduce int64

	// statistics, need to be 8 byte aligned, these statisitcs will be
	// flushed to the tip of indexfile.
	n_count    int64
	mnodes     int64
	znodes     int64
	dcount     int64
	a_zentries *lib.AverageInt64
	a_mentries *lib.AverageInt64
	a_keysize  *lib.AverageInt64
	a_valsize  *lib.AverageInt64
	a_redsize  *lib.AverageInt64
	h_depth    *lib.HistogramInt64

	indexfile string
	datafile  string
	indexfd   *os.File
	datafd    *os.File
	logprefix string

	// builder data
	iterator api.IndexIterator
	zpool    chan *zblock
	mpool    chan *mblock
	nodes    []api.Node
	flusher  *bubtflusher

	// configuration, will be flushed to the tip of indexfile.
	name         string
	mblocksize   int64
	zblocksize   int64
	mreduce      bool
	iterpoolsize int64
	level        byte
}

type blocker interface {
	startkey() (kpos int64, key []byte)
	reduce() []byte
	offset() int64
	backref() int64
	roffset() int64
}

// NewBubt create a Bubt instance to build a new bottoms-up btree.
func NewBubt(name, indexfile, datafile string, config lib.Config) *Bubt {
	var err error

	f := &Bubt{
		name:       name,
		zpool:      make(chan *zblock, zpoolSize),
		mpool:      make(chan *mblock, mpoolSize),
		nodes:      make([]api.Node, 0),
		a_zentries: &lib.AverageInt64{},
		a_mentries: &lib.AverageInt64{},
		a_keysize:  &lib.AverageInt64{},
		a_valsize:  &lib.AverageInt64{},
		a_redsize:  &lib.AverageInt64{},
		h_depth:    lib.NewhistorgramInt64(0, mpoolSize, 1),
	}
	f.logprefix = fmt.Sprintf("[BUBT-%s]", name)

	f.indexfile = indexfile
	if f.indexfd, err = os.Create(f.indexfile); err != nil {
		panic(err)
	}

	f.datafile = datafile
	if f.datafd, err = os.Create(f.datafile); err != nil {
		panic(err)
	}

	f.zblocksize = config.Int64("zblocksize")
	if f.zblocksize > maxBlock { // 1 TB
		log.Errorf("zblocksize %v > %v", f.zblocksize, maxBlock)
	} else if f.zblocksize < minBlock { // 512 byte, HDD sector size.
		log.Errorf("zblocksize %v < %v", f.zblocksize, minBlock)
	}
	f.mblocksize = config.Int64("mblocksize")
	if f.mblocksize > maxBlock {
		log.Errorf("mblocksize %v > %v", f.mblocksize, maxBlock)
	} else if f.mblocksize < minBlock {
		log.Errorf("mblocksize %v < %v", f.mblocksize, minBlock)
	}
	f.mreduce = config.Bool("mreduce")
	if f.hasdatafile() == false && f.mreduce == true {
		panic("cannot mreduce without datafile")
	}
	f.iterpoolsize = config.Int64("iterpool.size")
	f.level = byte(config.Int64("level"))

	f.flusher = f.startflusher()
	log.Infof("%v started ...", f.logprefix)
	return f
}

// Setlevel will set the storage level.
func (f *Bubt) Setlevel(level byte) {
	f.level = level
}

// Build starts building the tree from iterator, iterator is expected to be a
// full-table scan over another data-store.
func (f *Bubt) Build(iter api.IndexIterator) {
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
	f.rootblock, f.rootreduce = -1, -1
	if f.n_count > 0 {
		f.rootblock, f.rootreduce = block.backref(), block.roffset()
	}

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
	log.Infof("%v closing the iterator", f.logprefix)
	f.iterator.Close()
	log.Infof("%v ... build completed", f.logprefix)
}

func (f *Bubt) buildm(ms []*mblock, fpos [2]int64) ([]*mblock, blocker, [2]int64) {
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

func (f *Bubt) buildz(fpos [2]int64) (blocker, [2]int64) {
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

func (f *Bubt) flush(block blocker, fpos [2]int64) (blocker, [2]int64) {
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
			f.a_mentries.Add(int64(len(blk.entries)))
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

func (f *Bubt) pop() api.Node {
	if ln := len(f.nodes); ln > 0 {
		nd := f.nodes[ln-1]
		f.nodes = f.nodes[:ln-1]
		return nd
	}
	return f.iterator.Next()
}

func (f *Bubt) push(nd api.Node) {
	f.nodes = append(f.nodes, nd)
}

func (f *Bubt) hasdatafile() bool {
	return f.datafile != ""
}

func (f *Bubt) ismvpos(vpos int64) (int64, bool) {
	if (vpos & 0x1) == 1 {
		return int64(uint64(vpos) & 0xFFFFFFFFFFFFFFF8), true
	}
	return int64(uint64(vpos) & 0xFFFFFFFFFFFFFFF8), false
}

func (f *Bubt) config2json() []byte {
	config := map[string]interface{}{
		"name":       f.name,
		"zblocksize": f.zblocksize,
		"mblocksize": f.mblocksize,
		"mreduce":    f.mreduce,
		"level":      f.level,
	}
	data, err := json.Marshal(config)
	if err != nil {
		panic(err)
	}
	return data
}

func (f *Bubt) stats2json() []byte {
	stats := map[string]interface{}{
		"rootblock":  f.rootblock,
		"rootreduce": f.rootreduce,
		"n_count":    f.n_count,
		"mnodes":     f.mnodes,
		"znodes":     f.znodes,
		"a_zentries": f.a_zentries.Stats(),
		"a_mentries": f.a_mentries.Stats(),
		"a_keysize":  f.a_keysize.Stats(),
		"a_valsize":  f.a_valsize.Stats(),
		"a_redsize":  f.a_redsize.Stats(),
		"h_depth":    f.h_depth.Fullstats(),
	}
	data, err := json.Marshal(stats)
	if err != nil {
		panic(err)
	}
	return data
}
