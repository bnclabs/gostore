package bubt

import "encoding/binary"
import "encoding/json"
import "fmt"
import "path/filepath"
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

	path      string
	indexfile string
	datafile  string
	indexfd   *os.File
	datafd    *os.File
	logprefix string

	// builder data
	iterator api.IndexIterator
	nodes    []api.Node
	flusher  *bubtflusher

	// settings, will be flushed to the tip of indexfile.
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
func NewBubt(name, path string, setts lib.Settings) *Bubt {
	f := &Bubt{
		name:       name,
		path:       path,
		nodes:      make([]api.Node, 0),
		a_zentries: &lib.AverageInt64{},
		a_mentries: &lib.AverageInt64{},
		a_keysize:  &lib.AverageInt64{},
		a_valsize:  &lib.AverageInt64{},
		a_redsize:  &lib.AverageInt64{},
		h_depth:    lib.NewhistorgramInt64(0, mpoolSize, 1),
	}
	f.logprefix = fmt.Sprintf("[BUBT-%s]", name)

	if err := os.MkdirAll(path, 0770); err != nil {
		log.Errorf("%v MkdirAll(%q)", f.logprefix, path)
		return nil
	}

	indexfile, datafile := mkfilenames(path)
	f.indexfile, f.indexfd = indexfile, createfile(indexfile)
	if setts.Bool("datafile") {
		f.datafile, f.datafd = datafile, createfile(datafile)
	} else {
		f.datafile, f.datafd = "", nil
	}

	f.zblocksize = setts.Int64("zblocksize")
	if f.zblocksize > maxBlock { // 1 TB
		panic(fmt.Errorf("zblocksize %v > %v\n", f.zblocksize, maxBlock))
	} else if f.zblocksize < minBlock { // 512 byte, HDD sector size.
		panic(fmt.Errorf("zblocksize %v < %v\n", f.zblocksize, minBlock))
	}
	f.mblocksize = setts.Int64("mblocksize")
	if f.mblocksize > maxBlock {
		panic(fmt.Errorf("mblocksize %v > %v\n", f.mblocksize, maxBlock))
	} else if f.mblocksize < minBlock {
		panic(fmt.Errorf("mblocksize %v < %v\n", f.mblocksize, minBlock))
	}
	f.mreduce = setts.Bool("mreduce")
	if f.hasdatafile() == false && f.mreduce == true {
		panic("cannot mreduce without datafile")
	}
	f.iterpoolsize = setts.Int64("iterpool.size")
	f.level = byte(setts.Int64("level"))

	f.flusher = f.startflusher()
	return f
}

// Setlevel will set the storage level.
func (f *Bubt) Setlevel(level byte) {
	f.level = level
}

// Build starts building the tree from iterator, iterator is expected to be a
// full-table scan over another data-store.
func (f *Bubt) Build(iter api.IndexIterator) {
	log.Infof("%v builder started ...\n", f.logprefix)

	f.iterator = iter

	// add a new level to the btree.
	prependlevel := func(ms []*mblock, mblock *mblock) []*mblock {
		ln := len(ms)
		ms = append(ms, nil)
		copy(ms[1:], ms[:ln])
		ms[0] = mblock
		return ms
	}

	flushstats := func() {
		// flush statistics
		finblock := make([]byte, markerBlocksize)
		if stats := f.stats2json(); len(stats) > len(finblock) {
			panic(fmt.Errorf("stats %v > %v", len(stats), len(finblock)))

		} else {
			binary.BigEndian.PutUint16(finblock[:2], uint16(len(stats)))
			copy(finblock[2:], stats)
			if err := f.flusher.writeidx(finblock); err != nil {
				panic(fmt.Errorf("writing stats: %v", err))
			}
		}
		log.Infof("%v builder writing stat block\n", f.logprefix)
	}

	flushsettings := func() {
		finblock := make([]byte, markerBlocksize)
		binary.BigEndian.PutUint64(finblock[:8], uint64(f.rootblock))
		binary.BigEndian.PutUint64(finblock[8:16], uint64(f.rootreduce))
		if jsetts := f.setts2json(); len(jsetts) > len(finblock) {
			panic(fmt.Errorf("settings %v > %v", len(jsetts), len(finblock)))

		} else {
			binary.BigEndian.PutUint16(finblock[16:18], uint16(len(jsetts)))
			copy(finblock[18:], jsetts)
			if err := f.flusher.writeidx(finblock); err != nil {
				panic(fmt.Errorf("writing settings: %v", err))
			}
		}
		log.Infof("%v builder writing settings block\n", f.logprefix)
	}

	var block blocker
	var fin bool

	// build
	ms, fpos := []*mblock{}, [2]int64{0, 0}
	for ms, block, fpos, fin = f.buildm(ms, fpos); fin == false; {
		mblock := f.newmblock()
		if mblock.insert(block) == false {
			panic("inserting first entry into mblock")
		}
		ms, block, fpos, fin = f.buildm(prependlevel(ms, mblock), fpos)
	}

	if f.n_count == 0 {
		log.Infof("%v builder finds empty iterator\n", f.logprefix)
	}

	// root-block and its reduced value.
	f.rootblock, f.rootreduce = -1, -1
	if f.n_count > 0 {
		f.rootblock = block.backref()
		if f.mreduce {
			f.rootreduce = block.roffset()
		}
	}

	flushstats()
	flushsettings()

	// close and wait for datafile to be sealed.
	f.flusher.close()
	log.Infof("%v ... build completed\n", f.logprefix)
}

func (f *Bubt) Count() int64 {
	return f.n_count
}

func (f *Bubt) buildm(ms []*mblock, fpos [2]int64) ([]*mblock, blocker, [2]int64, bool) {
	var block blocker
	var fin bool
	var childms []*mblock

	if len(ms) == 0 {
		block, fpos, fin = f.buildz(fpos)
		return ms, block, fpos, fin
	}

	f.dcount++
	defer func() { f.dcount-- }()

	mblock := ms[0]
	if mblock == nil {
		return ms, nil, fpos, true
	}

	childms, block, fpos, fin = f.buildm(ms[1:], fpos)
	ok := mblock.insert(block)
	for ok && fin == false {
		childms, block, fpos, fin = f.buildm(childms, fpos)
		ok = mblock.insert(block)
	}

	_, fpos = f.flush(mblock, fpos)
	ms[0] = nil
	if block != nil && ok == false {
		ms[0] = f.newmblock()
		if block != nil && ms[0].insert(block) == false {
			panic("first insert to mblock, check whether key > mblocksize")
		}
		return ms, mblock, fpos, false
	}
	return ms, mblock, fpos, true
}

func (f *Bubt) buildz(fpos [2]int64) (blocker, [2]int64, bool) {
	z := f.newz(fpos)

	f.dcount++
	defer func() { f.dcount-- }()
	defer func() { f.h_depth.Add(f.dcount) }()

	nd := f.pop()
	ok, fin := z.insert(nd)
	if nd != nil && ok == false {
		panic("first insert to zblock, check whether key > zblocksize")
	}
	for ok { // if ok == true, fin shall be false
		nd = f.pop()
		ok, fin = z.insert(nd)
	}
	if nd != nil {
		f.push(nd)
	}
	block, newfpos := f.flush(z, fpos)
	return block, newfpos, fin
}

func (f *Bubt) flush(block blocker, fpos [2]int64) (blocker, [2]int64) {
	logprefix := f.logprefix
	switch blk := block.(type) {
	case *zblock:
		if len(blk.entries) > 0 {
			fmsg := "%v flush zblock %v entries\n"
			log.Debugf(fmsg, logprefix, len(blk.entries))

			f.znodes++

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
				panic(fmt.Errorf("flushing zblock data: %v", err))
			}
			kpos := fpos[0] + int64(len(blk.kbuffer))
			if int64(len(blk.kbuffer)) > f.zblocksize {
				fmsg := "zblock exceeds size %v"
				panic(fmt.Errorf(fmsg, len(blk.kbuffer), f.zblocksize))
			} else if err := f.flusher.writeidx(blk.kbuffer); err != nil {
				panic(fmt.Errorf("flushing zblock index: %v", err))
			}
			return blk, [2]int64{kpos, vpos}
		}
		fmsg := "%v flush skipping zblock %v entries\n"
		log.Debugf(fmsg, logprefix, len(blk.entries))
		return nil, fpos

	case *mblock:
		if len(blk.entries) > 0 {
			fmsg := "%v flush mblock %v entries\n"
			log.Debugf(fmsg, logprefix, len(blk.entries))

			f.mnodes++

			f.a_mentries.Add(int64(len(blk.entries)))
			blk.finalize()
			// reduce
			blk.fpos, blk.rpos = fpos, fpos[1]
			reducevalue := blk.reduce()
			f.a_redsize.Add(int64(len(reducevalue)))
			// move forward [2]fpos
			vpos := fpos[1] + int64(len(reducevalue))
			if err := f.flusher.writedata(reducevalue); err != nil {
				panic(fmt.Errorf("flushing mblock data: %v", err))
			}
			kpos := fpos[0] + int64(len(blk.kbuffer))
			if int64(len(blk.kbuffer)) > f.zblocksize {
				fmsg := "zblock exceeds size %v"
				panic(fmt.Errorf(fmsg, len(blk.kbuffer), f.zblocksize))
			} else if err := f.flusher.writeidx(blk.kbuffer); err != nil {
				panic(fmt.Errorf("flushing mblock index: %v", err))
			}
			return blk, [2]int64{kpos, vpos}
		}
		fmsg := "%v flush skipping mblock %v entries\n"
		log.Debugf(fmsg, logprefix, len(blk.entries))
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

func (f *Bubt) setts2json() []byte {
	setts := lib.Settings{
		"zblocksize":    f.zblocksize,
		"mblocksize":    f.mblocksize,
		"mreduce":       f.mreduce,
		"iterpool.size": f.iterpoolsize,
		"level":         f.level,
		"datafile":      f.datafile != "",
	}
	data, err := json.Marshal(setts)
	if err != nil {
		panic(fmt.Errorf("marshaling settings: %v", err))
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
		panic(fmt.Errorf("marshaling statistics: %v", err))
	}
	return data
}

func mkfilenames(path string) (string, string) {
	index, data := filepath.Join(path, "index"), filepath.Join(path, "data")
	return index, data
}

func createfile(name string) *os.File {
	os.Remove(name)
	fd, err := os.OpenFile(name, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		panic(fmt.Errorf("create append file: %v", err))
	}
	return fd
}
