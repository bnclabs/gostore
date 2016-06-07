package bubt

import "os"
import "fmt"
import "bytes"
import "errors"
import "sync"
import "sync/atomic"
import "encoding/json"
import "encoding/binary"

import "github.com/prataprc/storage.go/api"
import "github.com/prataprc/storage.go/lib"

var readmu sync.Mutex
var openstores map[string]*Snapshot

// Snapshot manages sorted key,value entries in persisted, immutable btree
// built bottoms up and not updated there after.
type Snapshot struct {
	// statisitcs, need to be 8 byte aligned.
	n_snapshots int64
	rootblock   int64
	rootreduce  int64
	n_count     int64

	indexfile string
	datafile  string
	indexfd   *os.File
	datafd    *os.File
	logprefix string

	// reader data
	builderstats map[string]interface{}
	znodepool    chan []byte
	mnodepool    chan []byte

	// configuration, will be flushed to the tip of indexfile.
	name       string
	mblocksize int64
	zblocksize int64
	mreduce    bool
	level      byte
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

	if ss.datafd, err = os.Open(ss.datafile); err != nil {
		panic(err)
	}
	if ss.indexfd, err = os.Open(ss.indexfile); err != nil {
		panic(err)
	}
	fi, err := ss.indexfd.Stat()
	if err != nil {
		panic(err)
	}
	eof := fi.Size()

	markerat := eof - markerBlocksize
	n, err := ss.indexfd.ReadAt(block, markerat)
	if err != nil {
		panic(err)
	} else if int64(n) != markerBlocksize {
		panic("%v partial read: %v != %v", ss.logprefix, n, markerBlocksize)
	} else {
		for _, byt := range block {
			if byt != 0xAB { // TODO: not magic numbers
				panic("invalid marker")
			}
		}
	}

	// load config block
	configat := markerat - markerBlocksize
	n, err = ss.indexfd.ReadAt(block, configat)
	if err != nil {
		panic(err)
	} else if int64(n) != markerBlocksize {
		panic("%v partial read: %v != %v", ss.logprefix, n, markerBlocksize)
	} else {
		ss.rootblock = int64(binary.BigEndian.Uint64(block[:8]))
		ss.rootreduce = int64(binary.BigEndian.Uint64(block[8:16]))
		ln := binary.BigEndian.Uint16(block[16:18])
		if err := ss.json2config(block[18 : 18+ln]); err != nil {
			panic(err)
		}
	}
	// validate config block
	if ss.name != name {
		panic(fmt.Errorf("expected name %v, got %v", ss.name, name))
	} else if ss.zblocksize != zblocksize {
		fmsg := "expected zblocksize %v, got %v"
		panic(fmt.Errorf(fmsg, ss.zblocksize, zblocksize))
	}

	// load stats block
	statat := configat - zblocksize
	n, err = ss.indexfd.ReadAt(block, statat)
	if err != nil {
		panic(err)
	} else if int64(n) != zblocksize {
		panic("%v partial read: %v != %v", ss.logprefix, n, zblocksize)
	} else {
		ln := binary.BigEndian.Uint16(block[:2])
		if err := ss.json2stats(block[2 : 2+ln]); err != nil {
			panic(err)
		}
	}

	ss.znodepool = make(chan []byte, zpoolSize)
	for i := 0; i < cap(ss.znodepool); i++ {
		ss.znodepool <- make([]byte, ss.zblocksize)
	}
	ss.mnodepool = make(chan []byte, mpoolSize)
	for i := 0; i < cap(ss.mnodepool); i++ {
		ss.mnodepool <- make([]byte, ss.mblocksize)
	}

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
		panic("active snapshots")
	}

	var errs string
	if err := ss.indexfd.Close(); err != nil {
		errs += err.Error()
	}
	if err := ss.datafd.Close(); err != nil {
		errs += "; " + err.Error()
	}
	if err := os.Remove(ss.indexfile); err != nil {
		errs += "; " + err.Error()
	}
	if err := os.Remove(ss.datafile); err != nil {
		errs += "; " + err.Error()
	}
	if errs != "" {
		return errors.New(errs)
	}
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
	return false
}

func (ss *Snapshot) Get(key []byte) api.Node {
	return nil
}

func (ss *Snapshot) Min() api.Node {
	return nil
}

func (ss *Snapshot) Max() api.Node {
	return nil
}

func (ss *Snapshot) Range(lowkey, highkey []byte, incl string, reverse bool, iter api.RangeCallb) {
}

func (ss *Snapshot) Iterate(lowkey, highkey []byte, incl string, reverse bool) api.IndexIterator {
	return nil
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
	ss.name = config.String("name")
	ss.zblocksize = config.Int64("zblocksize")
	ss.mblocksize = config.Int64("mblocksize")
	ss.mreduce = config.Bool("mreduce")
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

func (ss *Snapshot) rangekey(key []byte, fpos int64, cmp [2]int, callb api.RangeCallb) bool {
	switch ndblk := ss.readat(fpos).(type) {
	case mnode:
		var from int32

		entries := ndblk.entryslice()
		switch len(entries) {
		case 0:
			return false
		case 4:
			from = 0
		default:
			from = 1 + ndblk.searchkey(key, entries[4:], cmp[0])
		}
		for x := from; x < int32(len(entries)/4); x++ {
			vpos := ndblk.getentry(uint32(x), entries).vpos()
			if ss.rangekey(key, vpos, cmp, callb) == false {
				ss.mnodepool <- []byte(ndblk)
				return false
			}
		}
		ss.mnodepool <- []byte(ndblk)

	case znode:
		var nd node

		entries := ndblk.entryslice()
		from := ndblk.searchkey(key, entries, cmp[0])
		for x := from; x < int32(len(entries)/4); x++ {
			ge := bytes.Compare(key, ndblk.getentry(uint32(x), entries).key()) >= cmp[0]
			le := bytes.Compare(key, ndblk.getentry(uint32(x), entries).key()) >= cmp[1]
			if ge && le {
				koff := x * 4
				offset := fpos + int64(binary.BigEndian.Uint32(entries[koff:koff+4]))
				ss.newznode(&nd, []byte(ndblk), offset)
				if callb(&nd) == false {
					ss.znodepool <- []byte(ndblk)
					return false
				}

			} else if le == false {
				ss.znodepool <- []byte(ndblk)
				return false
			}
		}
		ss.znodepool <- []byte(ndblk)
	}
	return true
}

func (ss *Snapshot) readat(fpos int64) (nd interface{}) {
	var data []byte
	if vpos, mok := ss.ismvpos(fpos); mok {
		data = <-ss.mnodepool
		nd = mnode(data)
	} else {
		data = <-ss.znodepool
		nd = znode(data)
	}
	if n, err := ss.indexfd.ReadAt(data, fpos); err != nil {
		panic(err)
	} else if n != len(data) {
		panic("partial read")
	}
	return
}
