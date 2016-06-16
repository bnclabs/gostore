package bubt

import "os"
import "fmt"
import "bytes"
import "sync"
import "sync/atomic"
import "encoding/json"
import "encoding/binary"

import "github.com/prataprc/storage.go/api"
import "github.com/prataprc/storage.go/lib"
import "github.com/prataprc/storage.go/log"

var readmu sync.Mutex
var openstores = make(map[string]*Snapshot)

// Snapshot manages sorted key,value entries in persisted, immutable btree
// built bottoms up and not updated there after.
type Snapshot struct {
	rootblock  int64
	rootreduce int64

	// statisitcs, need to be 8 byte aligned.
	n_snapshots int64
	n_count     int64
	n_lookups   int64
	n_ranges    int64

	indexfile string
	datafile  string
	indexfd   *os.File
	datafd    *os.File
	logprefix string

	// reader data
	builderstats map[string]interface{}
	iterpool     chan *iterator
	activeiter   int64

	// configuration, will be flushed to the tip of indexfile.
	name         string
	mblocksize   int64
	zblocksize   int64
	mreduce      bool
	iterpoolsize int64
	level        byte
}

func OpenBubtstore(name, indexfile, datafile string, zblocksize int64) (ss *Snapshot, err error) {
	var ok bool

	readmu.Lock()
	defer readmu.Unlock()

	if ss, ok = openstores[name]; ok {
		return ss, nil
	}

	ss = &Snapshot{
		name:       name,
		indexfile:  indexfile,
		datafile:   datafile,
		zblocksize: zblocksize,
	}

	block := make([]byte, markerBlocksize)
	ss.logprefix = fmt.Sprintf("[BUBT-%s]", name)

	if err := ss.openfiles(); err != nil {
		return nil, err
	}

	fi, err := ss.indexfd.Stat()
	if err != nil {
		fmt.Println("debug", err)
		fmsg := "%v unable to stat %q: %v\n"
		log.Errorf(fmsg, ss.logprefix, ss.indexfile, err)
		return nil, err
	}
	eof := fi.Size()

	markerat := eof - markerBlocksize
	n, err := ss.indexfd.ReadAt(block, markerat)
	if err != nil {
		fmsg := "%v reading %q marker-block: %v\n"
		log.Errorf(fmsg, ss.logprefix, ss.indexfile, err)
		return nil, err

	} else if int64(n) != markerBlocksize {
		err := fmt.Errorf("partial read: %v != %v\n", n, markerBlocksize)
		log.Errorf("%v %v\n", ss.logprefix, err)
		return nil, err

	} else {
		for _, byt := range block {
			if byt != 0xAB { // TODO: not magic numbers
				err := fmt.Errorf("invalid marker")
				log.Errorf("%v %v\n", ss.logprefix, err)
				return nil, err
			}
		}
	}

	// load config block
	configat := markerat - markerBlocksize
	n, err = ss.indexfd.ReadAt(block, configat)
	if err != nil {
		panicerr("config ReadAt: %v", err)
	} else if int64(n) != markerBlocksize {
		panicerr("%v partial read: %v != %v", ss.logprefix, n, markerBlocksize)
	} else {
		ss.rootblock = int64(binary.BigEndian.Uint64(block[:8]))
		ss.rootreduce = int64(binary.BigEndian.Uint64(block[8:16]))
		ln := binary.BigEndian.Uint16(block[16:18])
		if err := ss.json2config(block[18 : 18+ln]); err != nil {
			panicerr("json2config: %v", err)
		}
	}
	// validate config block
	if ss.name != name {
		panicerr("expected name %v, got %v", ss.name, name)
	} else if ss.zblocksize != zblocksize {
		panicerr("expected zblocksize %v, got %v", ss.zblocksize, zblocksize)
	}

	// load stats block
	statat := configat - zblocksize
	n, err = ss.indexfd.ReadAt(block, statat)
	if err != nil {
		panicerr("stats ReadAt: %v", err)
	} else if int64(n) != zblocksize {
		panicerr("%v partial read: %v != %v", ss.logprefix, n, zblocksize)
	} else {
		ln := binary.BigEndian.Uint16(block[:2])
		if err := ss.json2stats(block[2 : 2+ln]); err != nil {
			panicerr("json2stats: %v", err)
		}
	}

	ss.iterpool = make(chan *iterator, ss.iterpoolsize)

	openstores[ss.name] = ss
	return ss, nil
}

//---- Index{} interface.

// ID implement Index{} interface.
func (ss *Snapshot) ID() string {
	return ss.name
}

// Count implement Index{} interface.
func (ss *Snapshot) Count() int64 {
	return ss.n_count
}

// Isactive implement Index{} interface.
func (ss *Snapshot) Isactive() bool {
	return atomic.LoadInt64(&ss.n_snapshots) > 0
}

// RSnapshot implement Index{} interface.
func (ss *Snapshot) RSnapshot(snapch chan api.IndexSnapshot) error {
	ss.Refer()
	go func() { snapch <- ss }()
	return nil
}

// Stats implement Index{} interface.
func (ss *Snapshot) Stats() (map[string]interface{}, error) {
	panic("TBD")
}

// Fullstats implement Index{} interface.
func (ss *Snapshot) Fullstats() (map[string]interface{}, error) {
	panic("TBD")
}

// Log implement Index{} interface.
func (ss *Snapshot) Log(involved int, humanize bool) {
	if atomic.LoadInt64(&ss.n_snapshots) == 0 {
		panic("TBD")
	}
	panic("all snapshots released")
}

// Validate implement Index{} interface.
func (ss *Snapshot) Validate() {
	if atomic.LoadInt64(&ss.n_snapshots) == 0 {
		panic("TBD")
	}
	panic("all snapshots released")
}

// Destroy implement Index{} interface.
func (ss *Snapshot) Destroy() error {
	if atomic.LoadInt64(&ss.n_snapshots) > 0 {
		panic("Destory(): active snapshots")
	} else if atomic.LoadInt64(&ss.activeiter) > 0 {
		panic("Destroy(): active iterators")
	}

	readmu.Lock()
	defer readmu.Unlock()

	if err := ss.indexfd.Close(); err != nil {
		log.Errorf("%v closing %q: %v\n", ss.logprefix, ss.indexfile, err)
		return err
	}
	if ss.datafd != nil {
		if err := ss.datafd.Close(); err != nil {
			log.Errorf("%v closing %q %v\n", ss.logprefix, ss.datafile, err)
			return err
		}
	}

	if err := os.Remove(ss.indexfile); err != nil {
		log.Errorf("%v while removing %q: %v\n", ss.logprefix, ss.indexfile, err)
		return err
	} else {
		log.Infof("%v removing %q\n", ss.logprefix, ss.indexfile)
	}

	if ss.datafile != "" {
		if err := os.Remove(ss.datafile); err != nil {
			log.Errorf("%v while removing %q: %v\n", ss.logprefix, ss.datafile, err)
			return err
		} else {
			log.Infof("%v removing %q\n", ss.logprefix, ss.datafile)
		}
	}

	delete(openstores, ss.name)
	return nil
}

//---- IndexSnapshot interface.

func (ss *Snapshot) Refer() {
	atomic.AddInt64(&ss.n_snapshots, 1)
}

func (ss *Snapshot) Release() {
	atomic.AddInt64(&ss.n_snapshots, -1)
}

//---- IndexReader{} interface.

func (ss *Snapshot) Has(key []byte) bool {
	return ss.Get(key, nil)
}

func (ss *Snapshot) Get(key []byte, callb api.NodeCallb) bool {
	if ss.n_count == 0 {
		return false
	}

	var rc bool
	ss.rangeforward(key, key, ss.rootblock, [2]int{0, 0}, func(nd api.Node) bool {
		rc = true
		if callb != nil {
			rc = callb(nd)
		}
		return false
	})
	atomic.AddInt64(&ss.n_lookups, 1)
	return rc
}

func (ss *Snapshot) Min(callb api.NodeCallb) bool {
	if ss.n_count == 0 {
		return false
	}

	var rc bool
	ss.rangeforward(nil, nil, ss.rootblock, [2]int{0, 0}, func(nd api.Node) bool {
		rc = true
		if callb != nil {
			rc = callb(nd)
		}
		return false
	})
	atomic.AddInt64(&ss.n_lookups, 1)
	return rc
}

func (ss *Snapshot) Max(callb api.NodeCallb) bool {
	if ss.n_count == 0 {
		return false
	}

	var rc bool
	ss.rangebackward(nil, nil, ss.rootblock, [2]int{0, 0}, func(nd api.Node) bool {
		rc = true
		if callb != nil {
			rc = callb(nd)
		}
		return false
	})
	atomic.AddInt64(&ss.n_lookups, 1)
	return rc
}

func (ss *Snapshot) Range(lkey, hkey []byte, incl string, reverse bool, callb api.RangeCallb) {
	if ss.rootblock < 0 {
		return
	}

	var cmp [2]int

	if reverse == false {
		switch incl {
		case "low":
			cmp = [2]int{0, -1}
		case "high":
			cmp = [2]int{1, 0}
		case "both":
			cmp = [2]int{0, 0}
		case "none":
			cmp = [2]int{1, -1}
		}
		ss.rangeforward(lkey, hkey, ss.rootblock, cmp, callb)
	} else {
		switch incl {
		case "low":
			cmp = [2]int{0, -1}
		case "high":
			cmp = [2]int{1, 0}
		case "both":
			cmp = [2]int{0, 0}
		case "none":
			cmp = [2]int{1, -1}
		}
		ss.rangebackward(lkey, hkey, ss.rootblock, cmp, callb)
	}
	atomic.AddInt64(&ss.n_lookups, 1)
	return
}

func (ss *Snapshot) Iterate(lkey, hkey []byte, incl string, r bool) api.IndexIterator {

	if lkey != nil && hkey != nil && bytes.Compare(lkey, hkey) == 0 {
		if incl == "none" {
			return nil
		} else if incl == "low" || incl == "high" {
			incl = "both"
		}
	}

	var iter *iterator
	select {
	case iter = <-ss.iterpool:
	default:
		iter = &iterator{}
	}

	// NOTE: always re-initialize, because we are getting it back from pool.
	iter.tree, iter.snapshot = ss, ss
	iter.nodes, iter.index, iter.limit = iter.nodes[:0], 0, 5
	iter.continuate = false
	iter.startkey, iter.endkey, iter.incl, iter.reverse = lkey, hkey, incl, r
	iter.closed, iter.activeiter = false, &ss.activeiter

	if iter.nodes == nil {
		iter.nodes = make([]api.Node, 0)
	}

	iter.rangefill()
	if r {
		switch iter.incl {
		case "none":
			iter.incl = "high"
		case "low":
			iter.incl = "both"
		}
	} else {
		switch iter.incl {
		case "none":
			iter.incl = "low"
		case "high":
			iter.incl = "both"
		}
	}

	atomic.AddInt64(&ss.n_ranges, 1)
	atomic.AddInt64(&ss.activeiter, 1)
	return iter
}

//---- IndexWriter interface{}

// Upsert IndexWriter{} method, will panic if called.
func (ss *Snapshot) Upsert(key, value []byte, callb api.UpsertCallback) error {
	panic("IndexWriter.Upsert() not implemented")
}

// UpsertMany IndexWriter{} method, will panic if called.
func (ss *Snapshot) UpsertMany(keys, values [][]byte, callb api.UpsertCallback) error {
	panic("IndexWriter.UpsertMany() not implemented")
}

// DeleteMin IndexWriter{} method, will panic if called.
func (ss *Snapshot) DeleteMin(callb api.DeleteCallback) error {
	panic("IndexWriter.DeleteMin() not implemented")
}

// DeleteMax IndexWriter{} method, will panic if called.
func (ss *Snapshot) DeleteMax(callb api.DeleteCallback) error {
	panic("IndexWriter.DeleteMax() not implemented")
}

// Delete IndexWriter{} method, will panic if called.
func (ss *Snapshot) Delete(key []byte, callb api.DeleteCallback) error {
	panic("IndexWriter.Delete() not implemented")
}

//---- helper methods.

func (ss *Snapshot) hasdatafile() bool {
	return ss.datafile != ""
}

func (ss Snapshot) ismvpos(vpos int64) (int64, bool) {
	if (vpos & 0x1) == 1 {
		return int64(uint64(vpos) & 0xFFFFFFFFFFFFFFF8), true
	}
	return int64(uint64(vpos) & 0xFFFFFFFFFFFFFFF8), false
}

func (ss *Snapshot) json2config(data []byte) error {
	var config lib.Config
	if err := json.Unmarshal(data, &config); err != nil {
		return err
	}
	ss.zblocksize = config.Int64("zblocksize")
	ss.mblocksize = config.Int64("mblocksize")
	ss.mreduce = config.Bool("mreduce")
	ss.iterpoolsize = config.Int64("iterpool.size")
	ss.level = byte(config.Int64("level"))
	return nil
}

func (ss *Snapshot) json2stats(data []byte) error {
	if err := json.Unmarshal(data, &ss.builderstats); err != nil {
		return err
	}
	ss.n_count = int64(ss.builderstats["n_count"].(float64))
	return nil
}

func (ss *Snapshot) rangeforward(
	lkey, hkey []byte, fpos int64, cmp [2]int, callb api.RangeCallb) bool {

	switch ndblk := ss.readat(fpos).(type) {
	case mnode:
		rc := ndblk.rangeforward(ss, lkey, hkey, cmp, callb)
		return rc

	case znode:
		rc := ndblk.rangeforward(ss, lkey, hkey, fpos, cmp, callb)
		return rc
	}
	return true
}

func (ss *Snapshot) rangebackward(
	lkey, hkey []byte, fpos int64, cmp [2]int, callb api.RangeCallb) bool {

	switch ndblk := ss.readat(fpos).(type) {
	case mnode:
		rc := ndblk.rangebackward(ss, lkey, hkey, cmp, callb)
		return rc

	case znode:
		rc := ndblk.rangebackward(ss, lkey, hkey, fpos, cmp, callb)
		return rc
	}
	return true
}

func (ss *Snapshot) readat(fpos int64) (nd interface{}) {
	var data []byte
	vpos, mok := ss.ismvpos(fpos)
	if mok {
		data = make([]byte, ss.mblocksize)
		nd = mnode(data)
	} else {
		data = make([]byte, ss.zblocksize)
		nd = znode(data)
	}
	if n, err := ss.indexfd.ReadAt(data, vpos); err != nil {
		panicerr("ReadAt %q: %v", ss.indexfile, err)
	} else if n != len(data) {
		panicerr("ReadAt %q : partial read", ss.indexfile)
	}
	return
}

func (ss *Snapshot) dumpkeys(fpos int64, prefix string) {
	switch ndblk := ss.readat(fpos).(type) {
	case mnode:
		ndblk.dumpkeys(ss, prefix)

	case znode:
		ndblk.dumpkeys(ss, prefix)
	}
}

func (ss *Snapshot) openfiles() (err error) {
	if ss.datafile != "" {
		if _, err := os.Stat(ss.datafile); os.IsNotExist(err) {
			log.Errorf("%v file %q not present\n", ss.logprefix, ss.datafile)
			return err
		}
	}
	if _, err := os.Stat(ss.indexfile); os.IsNotExist(err) {
		log.Errorf("%v file %q not present\n", ss.logprefix, ss.indexfile)
		return err
	}

	if ss.datafile != "" {
		ss.datafd, err = os.OpenFile(ss.datafile, os.O_RDONLY, 0666)
		if err != nil {
			fmsg := "%v datafile %q (os.O_RDONLY, 0666): %v\n"
			log.Errorf(fmsg, ss.logprefix, ss.datafile, err)
			return err
		}
	}
	ss.indexfd, err = os.OpenFile(ss.indexfile, os.O_RDONLY, 0666)
	if err != nil {
		fmsg := "%v datafile %q (os.O_RDONLY, 0666): %v\n"
		log.Errorf(fmsg, ss.logprefix, ss.datafile, err)
		return err
	}
	return nil
}