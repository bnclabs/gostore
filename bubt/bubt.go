package bubt

import "os"
import "fmt"
import "errors"
import "encoding/json"

import "github.com/prataprc/storage.go/api"
import "github.com/prataprc/storage.go/log"
import "github.com/prataprc/storage.go/lib"

// Bubtstore manages sorted key,value entries in persisted, immutable btree
// built bottoms up and not updated there after.
type Bubtstore struct {
	// reader statisitcs, need to be 8 byte aligned.
	n_snapshots int64

	// builder statistics, need to be 8 byte aligned, these statisitcs will be
	// flushed to the tip of indexfile.
	rootblock  int64
	rootreduce int64
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

	state     string
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

type blocker interface {
	startkey() (kpos int64, key []byte)
	reduce() []byte
	offset() int64
	backref() int64
	roffset() int64
}

// NewBubtstore create a Bubtstore instance to build a new bottoms-up btree.
func NewBubtstore(name, indexfile, datafile string, config lib.Config) *Bubtstore {
	var err error

	f := &Bubtstore{
		name:       name,
		state:      "create",
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

	f.flusher = f.startflusher()
	log.Infof("%v started ...", f.logprefix)
	return f
}

// Setlevel will set the storage level.
func (f *Bubtstore) Setlevel(level byte) {
	f.level = level
}

//---- Index{} interface.

// ID implement Index{} interface.
func (f *Bubtstore) ID() string {
	return f.name
}

// Count implement Index{} interface.
func (f *Bubtstore) Count() int64 {
	return f.n_count
}

// Isactive implement Index{} interface.
func (f *Bubtstore) Isactive() bool {
	return f.state == "active"
}

// RSnapshot implement Index{} interface.
func (f *Bubtstore) RSnapshot(snapch chan IndexSnapshot) error {
	f.Refer()
	go func() { snapch <- f }()
	return nil
}

// Stats implement Index{} interface.
func (f *Bubtstore) Stats() (map[string]interface{}, error) {
	panic("TBD")
}

// Fullstats implement Index{} interface.
func (f *Bubtstore) Fullstats() (map[string]interface{}, error) {
	panic("TBD")
}

// Log implement Index{} interface.
func (f *Bubtstore) Log(involved int, humanize bool) {
	if f.state == "active" || f.state == "ready" {
		panic("TBD")
	}
	panic("not in active or ready state")
}

// Validate implement Index{} interface.
func (f *Bubtstore) Validate() {
	if f.state == "active" || f.state == "ready" {
		panic("TBD")
	}
	panic("not in active or ready state")
}

// Destroy implement Index{} interface.
func (f *Bubtstore) Destroy() error {
	if atomic.LoadInt64(&f.n_snapshots) > 0 {
		panic("active snapshots")
	}

	var errs string
	if err := f.indexfd.Close(); err != nil {
		errs += err.Error()
	}
	if err := f.datafd.Close(); err != nil {
		errs += "; " + err.Error()
	}
	if err := os.Remove(f.indexfile); err != nil {
		errs += "; " + err.Error()
	}
	if err := os.Remove(f.datafile); err != nil {
		errs += "; " + err.Error()
	}
	if errs != "" {
		return errors.New(errs)
	}
	return nil
}

//---- IndexSnapshot interface.

func (f *Bubtstore) Refer() {
	atomic.AddInt64(&f.n_snapshots, 1)
}

func (f *Bubtstore) Release() {
	atomic.AddInt64(&f.n_snapshots, -1)
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

func (f Bubtstore) ismvpos(vpos int64) (int64, bool) {
	if (vpos & 0x1) == 1 {
		return int64(uint64(vpos) & 0xFFFFFFFFFFFFFFF8), true
	}
	return int64(uint64(vpos) & 0xFFFFFFFFFFFFFFF8), false
}

func (f *Bubtstore) config2json() []byte {
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

func (f *Bubtstore) json2config(data []byte) error {
	var config lib.Config
	if err := json.Unmarshal(data, &config); err != nil {
		return err
	}
	f.name = config.String("name")
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
		"n_count":    f.z.n_count,
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

func (f *Bubtstore) json2stats(data []byte) error {
	if err := json.Unmarshal(data, &f.builderstats); err != nil {
		return err
	}
	f.n_count = int64(f.builderstats["n_count"].(float64))
	return nil
}
