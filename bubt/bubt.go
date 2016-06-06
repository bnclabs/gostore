package bubt

import "os"
import "fmt"
import "encoding/json"

import "github.com/prataprc/storage.go/api"
import "github.com/prataprc/storage.go/log"
import "github.com/prataprc/storage.go/lib"

// Bubtstore manages sorted key,value entries in persisted, immutable btree
// built bottoms up and not updated there after.
type Bubtstore struct {
	// statistics, need to be 8 byte aligned, these statisitcs will be flushed
	// to the tip of indexfile.
	rootblock  int64
	rootreduce int64
	mnodes     int64
	znodes     int64
	dcount     int64
	a_zentries *lib.AverageInt64
	a_mentries *lib.AverageInt64
	a_keysize  *lib.AverageInt64
	a_valsize  *lib.AverageInt64
	a_redsize  *lib.AverageInt64
	h_depth    *lib.HistogramInt64

	indexfd   *os.File
	datafd    *os.File
	logprefix string

	// builder data
	iterator api.IndexIterator
	zpool    chan *bubtzblock
	mpool    chan *bubtmblock
	nodes    []api.Node
	flusher  *bubtflusher

	// configuration, will be flushed to the tip of indexfile.
	name       string
	indexfile  string
	datafile   string
	mblocksize int64
	zblocksize int64
	mreduce    bool
	level      byte
}

type bubtblock interface {
	startkey() (kpos int64, key []byte)
	reduce() []byte
	offset() int64
	roffset() int64
}

// NewBubtstore create a Bubtstore instance to build a new bottoms-up btree.
func NewBubtstore(name string, config lib.Config) *Bubtstore {
	var err error

	f := &Bubtstore{
		name:       name,
		zpool:      make(chan *bubtzblock, bubtZpoolSize),
		mpool:      make(chan *bubtmblock, bubtMpoolSize),
		nodes:      make([]api.Node, 0),
		a_zentries: &lib.AverageInt64{},
		a_mentries: &lib.AverageInt64{},
		a_keysize:  &lib.AverageInt64{},
		a_valsize:  &lib.AverageInt64{},
		a_redsize:  &lib.AverageInt64{},
		h_depth:    lib.NewhistorgramInt64(0, bubtMpoolSize, 1),
	}
	f.logprefix = fmt.Sprintf("[BUBT-%s]", name)

	f.indexfile = config.String("indexfile")
	if f.indexfd, err = os.Create(f.indexfile); err != nil {
		panic(err)
	}

	f.datafile = config.String("datafile")
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

	f.flusher = f.startflusher()
	log.Infof("%v started ...", f.logprefix)
	return f
}

// Setlevel will set the storage level.
func (f *Bubtstore) Setlevel(level byte) {
	f.level = level
}

//---- IndexWriter interface{}

// Upsert IndexWriter{} method, will panic if called.
func (f *Bubtstore) Upsert(key, value []byte, callb api.UpsertCallback) error {
	panic("IndexWriter.Upsert() not implemented")
}

// UpsertMany IndexWriter{} method, will panic if called.
func (f *Bubtstore) UpsertMany(keys, values [][]byte, callb api.UpsertCallback) error {
	panic("IndexWriter.UpsertMany() not implemented")
}

// DeleteMin IndexWriter{} method, will panic if called.
func (f *Bubtstore) DeleteMin(callb api.DeleteCallback) error {
	panic("IndexWriter.DeleteMin() not implemented")
}

// DeleteMax IndexWriter{} method, will panic if called.
func (f *Bubtstore) DeleteMax(callb api.DeleteCallback) error {
	panic("IndexWriter.DeleteMax() not implemented")
}

// Delete IndexWriter{} method, will panic if called.
func (f *Bubtstore) Delete(key []byte, callb api.DeleteCallback) error {
	panic("IndexWriter.Delete() not implemented")
}

//---- helper methods.

func (f *Bubtstore) hasdatafile() bool {
	return f.datafile != ""
}

func (f *Bubtstore) makemvpos(vpos int64) int64 {
	if (vpos & 0x7) != 0 {
		panic(fmt.Errorf("vpos %v expected to 8-bit aligned", vpos))
	}
	return vpos | 0x1
}

func (f Bubtstore) ismvpos(vpos int64) (int64, bool) {
	if (vpos & 0x1) == 1 {
		return int64(uint64(vpos) & 0xFFFFFFFFFFFFFFF8), true
	}
	return int64(uint64(vpos) & 0xFFFFFFFFFFFFFFF8), false
}

func (f *Bubtstore) config2json() []byte {
	config := map[string]interface{}{
		"name":       f.name,
		"indexfile":  f.indexfile,
		"datafile":   f.datafile,
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

func (f *Bubtstore) json2config(data []byte) error {
	var config lib.Config
	if err := json.Unmarshal(data, &config); err != nil {
		return err
	}
	f.name = config.String("name")
	f.indexfile = config.String("indexfile")
	f.datafile = config.String("datafile")
	f.zblocksize = config.Int64("zblocksize")
	f.mblocksize = config.Int64("mblocksize")
	f.mreduce = config.Bool("mreduce")
	f.level = byte(config.Int64("level"))
	return nil
}

func (f *Bubtstore) stats2json() []byte {
	stats := map[string]interface{}{
		"rootblock":  f.rootblock,
		"rootreduce": f.rootreduce,
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
