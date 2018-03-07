package bogn

import "io"
import "os"
import "fmt"
import "sort"
import "sync"
import "bytes"
import "time"
import "unsafe"
import "reflect"
import "strings"
import "strconv"
import "runtime"
import "math/rand"
import "io/ioutil"
import "sync/atomic"
import "encoding/json"
import "path/filepath"

import "github.com/bnclabs/gostore/api"
import "github.com/bnclabs/gostore/lib"
import "github.com/bnclabs/gostore/llrb"
import "github.com/bnclabs/gostore/bubt"
import s "github.com/bnclabs/gosettings"
import humanize "github.com/dustin/go-humanize"

// TODO: enable count aggregation across snapshots, with data-structures
// that support LSM it is difficult to maintain accurate count.

// Bogn instance to index key,value pairs.
type Bogn struct {
	// atomic access, 8-byte aligned
	nroutines int64
	dgmstate  int64
	snapspin  uint64

	name         string
	epoch        time.Time
	snapshot     unsafe.Pointer // *snapshot
	memversions  [3]int
	diskversions [16]int
	finch        chan struct{}
	snaprw       sync.RWMutex
	txnmeta

	// bogn settings
	logpath       string
	memstore      string
	diskstore     string
	durable       bool
	dgm           bool
	workingset    bool
	flushratio    float64
	compactratio  float64
	flushperiod   time.Duration
	compactperiod time.Duration
	memcapacity   int64
	setts         s.Settings
	logprefix     string
}

// PurgeIndex will purge all the disk level snapshots for index `name`
// founder under `diskpaths`.
func PurgeIndex(name, logpath, diskstore string, diskpaths []string) {
	bogn := &Bogn{name: name}
	bogn.logprefix = fmt.Sprintf("BOGN [%v]", name)
	bogn.destroydisksnaps(logpath, diskstore, diskpaths)
	return
}

// CompactIndex will remove older versions of disk level snapshots and
// if merge is true, will merge all disk-levels into single level.
func CompactIndex(name, diskstore string, diskpaths []string, merge bool) {
	bogn := &Bogn{name: name, diskstore: diskstore, snapshot: nil}
	bogn.logprefix = fmt.Sprintf("BOGN [%v]", name)
	bogn.compactdisksnaps(diskstore, diskpaths, merge)
	return
}

// New create a new bogn instance.
func New(name string, setts s.Settings) (*Bogn, error) {
	bogn := (&Bogn{
		name:      name,
		logprefix: fmt.Sprintf("BOGN [%v]", name),
	}).readsettings(setts)
	bogn.inittxns()
	bogn.epoch = time.Now()
	if err := bogn.makepaths(setts); err != nil {
		return nil, err
	}
	bogn.finch = make(chan struct{})

	startedat := bogn.epoch.Format(time.RFC3339Nano)
	infof("%v starting epoch@%v ...", bogn.logprefix, startedat)

	merge := false
	CompactIndex(bogn.name, bogn.diskstore, bogn.getdiskpaths(), merge)

	disks, err := bogn.opendisksnaps(setts)
	if err != nil {
		bogn.Close()
		return nil, err
	}
	// NOTE: If settings have changed in between a re-boot from disk,
	// user should use a migration tool to move disk snapshots
	// from older settings to new settings.
	lastseqno := bogn.loaddisksettings(disks[:])

	mw := bogn.warmupfromdisk(disks[:])

	head, err := opensnapshot(bogn, mw, disks, lastseqno)
	if err != nil {
		bogn.Close()
		return nil, err
	}
	head.refer()
	bogn.setheadsnapshot(head)

	return bogn, nil
}

// IMPORTANT: when ever this functin is updated, please update
// settingsfromdisk(), loaddisksettings(), settingstodisk() and
// validatesettings().
func (bogn *Bogn) readsettings(setts s.Settings) *Bogn {
	bogn.logpath = setts.String("logpath")
	bogn.memstore = setts.String("memstore")
	bogn.diskstore = setts.String("diskstore")
	bogn.durable = setts.Bool("durable")
	bogn.dgm = setts.Bool("dgm")
	bogn.workingset = setts.Bool("workingset")
	bogn.flushratio = setts.Float64("flushratio")
	bogn.compactratio = setts.Float64("compactratio")
	bogn.flushperiod = time.Duration(setts.Int64("flushperiod"))
	bogn.flushperiod *= time.Second
	bogn.compactperiod = time.Duration(setts.Int64("compactperiod"))
	bogn.compactperiod *= time.Second
	bogn.setts = setts

	atomic.StoreInt64(&bogn.dgmstate, 0)
	if bogn.dgm {
		atomic.StoreInt64(&bogn.dgmstate, 1)
	}

	// validate
	switch bogn.memstore {
	case "llrb", "mvcc":
	default:
		panic(fmt.Errorf("invalid memstore %q", bogn.memstore))
	}
	switch bogn.diskstore {
	case "bubt":
	default:
		panic(fmt.Errorf("invalid diskstore %q", bogn.diskstore))
	}

	// pick a logpath, if not supplied, from bubt-diskpaths.
	if bogn.durable {
		if len(bogn.logpath) == 0 {
			switch bogn.diskstore {
			case "bubt":
				diskpaths := bogn.getdiskpaths()
				if len(diskpaths) == 0 {
					panic(fmt.Errorf("missing bubt `diskpaths` settings"))
				}
				bogn.logpath = diskpaths[rand.Intn(10000)%len(diskpaths)]

			default:
				panic(fmt.Errorf("invalid diskstore %q", bogn.diskstore))
			}
		}
		if len(bogn.logpath) == 0 {
			panic("unable to pick/locate a logdir")
		}
		infof("%v logpath: %q", bogn.logprefix, bogn.logpath)
	}

	return bogn.readmemsettings(setts)
}

func (bogn *Bogn) readmemsettings(setts s.Settings) *Bogn {
	switch bogn.memstore {
	case "llrb", "mvcc":
		llrbsetts := bogn.setts.Section("llrb.").Trim("llrb.")
		bogn.memcapacity = llrbsetts.Int64("memcapacity")
	}
	return bogn
}

func (bogn *Bogn) settingstodisk() s.Settings {
	memversions := bogn.memversions
	diskversions := bogn.diskversions
	setts := s.Settings{
		"logpath":       bogn.logpath,
		"memstore":      bogn.memstore,
		"diskstore":     bogn.diskstore,
		"workingset":    bogn.workingset,
		"flushratio":    bogn.flushratio,
		"compactratio":  bogn.compactratio,
		"flushperiod":   bogn.flushperiod,
		"compactperiod": bogn.compactperiod,
		"memversions":   memversions,
		"diskversions":  diskversions,
	}
	llrbsetts := bogn.setts.Section("llrb.")
	bubtsetts := bogn.setts.Section("bubt.")
	setts = (s.Settings{}).Mixin(setts, llrbsetts, bubtsetts)
	return setts
}

// priority of settings.
// bogn-settings - settings from application passed to New() take priority.
// llrb-settings - settings from application passed to New() take priority.
// bubt-settings - settings from ndisk take priority.
func (bogn *Bogn) loaddisksettings(disks []api.Index) (seqno uint64) {
	alldisks := []api.Index{}
	for i, disk := range disks {
		if disk == nil {
			continue
		}
		level, _, _ := bogn.path2level(disk.ID())
		if level != i {
			panic(fmt.Errorf("expected level %v, got %v", i, level))
		}
		alldisks = append(alldisks, disk)
	}
	if len(alldisks) > 0 {
		disksetts := bogn.settingsfromdisk(alldisks[0])
		bogn.memversions = disksetts["memversions"].([3]int)
		bogn.diskversions = disksetts["diskversions"].([16]int)
		bogn.logpath = disksetts.String("logpath")
		bogn.validatesettings(disksetts)
		return bogn.getdiskseqno(alldisks[0])
	}
	return 0
}

func (bogn *Bogn) validatesettings(disksetts s.Settings) {
	setts := bogn.setts
	if memstore := disksetts.String("memstore"); memstore != bogn.memstore {
		fmsg := "found memstore:%q on disk, expected %q"
		panic(fmt.Errorf(fmsg, memstore, bogn.memstore))
	}
	diskstore := disksetts.String("diskstore")
	if diskstore != bogn.diskstore {
		fmsg := "found diskstore:%q on disk, expected %q"
		panic(fmt.Errorf(fmsg, diskstore, bogn.diskstore))
	}
	if bogn.durable {
		if logpath := disksetts.String("logpath"); logpath != bogn.logpath {
			fmsg := "found logpath:%q on disk, expected %q"
			panic(fmt.Errorf(fmsg, logpath, bogn.logpath))
		}
	}

	// bubt settings
	diskpaths1 := disksetts.Strings("bubt.diskpaths")
	sort.Strings(diskpaths1)
	diskpaths2 := setts.Strings("bubt.diskpaths")
	sort.Strings(diskpaths2)
	if reflect.DeepEqual(diskpaths1, diskpaths2) == false {
		fmsg := "found diskpaths:%v on disk, expected %v"
		panic(fmt.Errorf(fmsg, diskpaths1, diskpaths2))
	}
	msize1, msize2 := disksetts.Int64("bubt.msize"), setts.Int64("bubt.msize")
	if msize1 != msize2 {
		fmsg := "found msize:%v on disk, expected %v"
		panic(fmt.Errorf(fmsg, msize1, msize2))
	}
	zsize1, zsize2 := disksetts.Int64("bubt.zsize"), setts.Int64("bubt.zsize")
	if zsize1 != zsize2 {
		fmsg := "found zsize:%v on disk, expected %v"
		panic(fmt.Errorf(fmsg, zsize1, zsize2))
	}
	mmap1, mmap2 := disksetts.Bool("bubt.mmap"), disksetts.Bool("bubt.mmap")
	if mmap1 != mmap2 {
		fmsg := "found mmap:%v on disk, expected %v"
		panic(fmt.Errorf(fmsg, mmap1, mmap2))
	}
}

func (bogn *Bogn) settingsfromdisk(disk api.Index) s.Settings {
	switch d := disk.(type) {
	case *bubt.Snapshot:
		metadata := s.Settings(bogn.diskmetadata(d))
		return metadata
	}
	panic("unreachable code")
}

// create a new in-memory snapshot from latest disk snapshot, if
// there is not enough memory to hold the latest disk snapshot
// return nil.
func (bogn *Bogn) warmupfromdisk(disks []api.Index) api.Index {
	var ndisk api.Index

	for _, ndisk = range disks {
		if ndisk != nil {
			break
		}
	}
	if ndisk == nil {
		return nil
	}

	var entries, memcapacity, footprint int64

	switch bogn.diskstore {
	case "bubt":
		entries = ndisk.(*bubt.Snapshot).Count()
		footprint = ndisk.(*bubt.Snapshot).Footprint()
	}

	switch bogn.memstore {
	case "llrb":
		llrbsetts := bogn.setts.Section("llrb.").Trim("llrb.")
		memcapacity = llrbsetts.Int64("memcapacity")
		nodesize := int64(unsafe.Sizeof(llrb.Llrbnode{})) - 8
		if expected := (nodesize * 2) * entries; expected < memcapacity {
			return bogn.llrbfromdisk(ndisk, entries, footprint)
		} else {
			bogn.dgmstate = 1
		}

	case "mvcc":
		llrbsetts := bogn.setts.Section("llrb.").Trim("llrb.")
		memcapacity = llrbsetts.Int64("memcapacity")
		nodesize := int64(unsafe.Sizeof(llrb.Llrbnode{})) - 8
		if expected := (nodesize * 2) * entries; expected < memcapacity {
			return bogn.mvccfromdisk(ndisk, entries, footprint)
		} else {
			bogn.dgmstate = 1
		}

	default:
		panic("unreachable code")
	}

	fmsg := "%v memory capacity %v too small to warmup %v, %v entries"
	arg1 := humanize.Bytes(uint64(footprint))
	arg2 := humanize.Bytes(uint64(footprint))
	infof(fmsg, bogn.logprefix, arg1, arg2, entries)
	return nil
}

func (bogn *Bogn) llrbfromdisk(
	ndisk api.Index, entries, footprint int64) api.Index {

	now := time.Now()

	bogn.memversions[0]++
	iter, seqno := ndisk.Scan(), bogn.getdiskseqno(ndisk)
	name := bogn.memlevelname("mw", bogn.memversions[0])
	llrbsetts := bogn.setts.Section("llrb.").Trim("llrb.")
	mw := llrb.LoadLLRB(name, llrbsetts, iter)
	mw.Setseqno(seqno)
	iter(true /*fin*/)

	fmsg := "%v warmup LLRB %v (%v) %v entries -> %v in %v"
	arg1 := humanize.Bytes(uint64(footprint))
	took := time.Since(now).Round(time.Second)
	infof(fmsg, bogn.logprefix, ndisk.ID(), arg1, entries, mw.ID(), took)

	return mw
}

func (bogn *Bogn) mvccfromdisk(
	ndisk api.Index, entries, footprint int64) api.Index {

	now := time.Now()

	bogn.memversions[0]++
	iter, seqno := ndisk.Scan(), bogn.getdiskseqno(ndisk)
	name := bogn.memlevelname("mw", bogn.memversions[0])
	llrbsetts := bogn.setts.Section("llrb.").Trim("llrb.")
	mw := llrb.LoadMVCC(name, llrbsetts, iter)
	mw.Setseqno(seqno)
	iter(true /*fin*/)

	fmsg := "%v warmup MVCC %v (%v) %v entries -> %v in %v"
	arg1 := humanize.Bytes(uint64(footprint))
	took := time.Since(now).Round(time.Second)
	infof(fmsg, bogn.logprefix, ndisk.ID(), arg1, entries, mw.ID(), took)

	return mw
}

// Start bogn service. Typically bogn instances are created and
// started as:
//   inst := NewBogn("storage", setts).Start()
func (bogn *Bogn) Start() *Bogn {
	go purger(bogn)
	go compactor(bogn)

	// wait until all routines have started.
	for atomic.LoadInt64(&bogn.nroutines) < 2 {
		runtime.Gosched()
	}
	return bogn
}

func (bogn *Bogn) makepaths(setts s.Settings) error {
	var diskpaths []string

	switch bogn.diskstore {
	case "bubt":
		diskpaths = bogn.getdiskpaths()
	default:
		panic("impossible situation")
	}

	for _, path := range diskpaths {
		if err := os.MkdirAll(path, 0775); err != nil {
			errorf("%v %v", bogn.logprefix, err)
			return err
		}
	}

	// create logpath, please do this after creating `diskpaths`
	// because logpath might be one of the diskpaths.
	if bogn.durable {
		logdir := bogn.logdir(bogn.logpath)
		if err := os.MkdirAll(logdir, 0775); err != nil {
			errorf("%v %v", bogn.logprefix, err)
			return err
		}
	}
	return nil
}

func (bogn *Bogn) currsnapshot() *snapshot {
	return (*snapshot)(atomic.LoadPointer(&bogn.snapshot))
}

func (bogn *Bogn) setheadsnapshot(snapshot *snapshot) {
	atomic.StorePointer(&bogn.snapshot, unsafe.Pointer(snapshot))
}

func (bogn *Bogn) latestsnapshot() *snapshot {
	for {
		snap := bogn.currsnapshot()
		if snap == nil {
			return nil
		}
		snap.refer()
		if snap.istrypurge() == false {
			return snap
		}
		snap.release()
		runtime.Gosched()
	}
	panic("unreachable code")
}

var writelatch uint64 = 0x8000000000000000
var writelock uint64 = 0xc000000000000000

func (bogn *Bogn) snaprlock() {
	for {
		l := atomic.AddUint64(&bogn.snapspin, 1)
		if l&writelatch == 0 {
			return
		}
		// write latch is on
		atomic.AddUint64(&bogn.snapspin, ^uint64(0))
		runtime.Gosched()
	}
}

func (bogn *Bogn) snaprunlock() {
	l := atomic.AddUint64(&bogn.snapspin, ^uint64(0)) & writelock
	if l == 0 || l == writelatch {
		return
	}
	panic("impossible situation")
}

func (bogn *Bogn) snaplock() {
	setwritelatch := func() {
		for {
			old := atomic.LoadUint64(&bogn.snapspin)
			new := old | writelatch
			if atomic.CompareAndSwapUint64(&bogn.snapspin, old, new) {
				return
			}
			runtime.Gosched()
		}
	}

	for {
		setwritelatch()
		if old := atomic.LoadUint64(&bogn.snapspin); old == writelatch {
			new := old | writelock
			if atomic.CompareAndSwapUint64(&bogn.snapspin, old, new) {
				return
			}
		}
		runtime.Gosched()
	}
}

func (bogn *Bogn) snapunlock() {
	for {
		if atomic.CompareAndSwapUint64(&bogn.snapspin, writelock, 0) {
			return
		}
	}
}

func (bogn *Bogn) mwmetadata(
	seqno uint64, flushunix string, settstodisk s.Settings) []byte {

	if len(flushunix) == 0 {
		flushunix = fmt.Sprintf(`"%v"`, uint64(time.Now().Unix()))
	}
	metadata := map[string]interface{}{
		"seqno":     fmt.Sprintf(`"%v"`, seqno),
		"flushunix": flushunix,
	}
	setts := (s.Settings{}).Mixin(settstodisk, metadata)
	setts = setts.AddPrefix("bogn.")
	data, err := json.Marshal(setts)
	if err != nil {
		panic(err)
	}
	return data
}

func (bogn *Bogn) flushelapsed() bool {
	snap := bogn.currsnapshot()
	if snap == nil {
		return false
	}
	_, disk := snap.latestlevel()
	if disk == nil {
		return int64(time.Since(bogn.epoch)) > int64(bogn.flushperiod)
	}
	metadata := bogn.diskmetadata(disk)
	x, _ := strconv.Atoi(strings.Trim(metadata["flushunix"].(string), `"`))
	return time.Now().Sub(time.Unix(int64(x), 0)) > bogn.flushperiod
}

// should not overlap with disk0.
func (bogn *Bogn) pickflushdisk(disk0 api.Index) (disk api.Index, nlevel int) {
	snap := bogn.currsnapshot()

	latestlevel, latestdisk := snap.latestlevel()
	if latestlevel < 0 && disk0 != nil {
		panic("impossible situation")

	} else if latestlevel < 0 { // first time flush.
		nlevel := len(snap.disks) - 1
		disk := snap.disks[nlevel]
		return disk, nlevel

	} else if latestdisk != nil && disk0 != nil {
		level0, _, _ := bogn.path2level(disk0.ID())
		if latestlevel > level0 {
			panic("impossible situation")

		} else if latestlevel == level0 { // fall back by one level.
			if latestlevel == 0 {
				panic("all levels are exhausted")
			}
			return nil, latestlevel - 1
		}
	}
	footprint := float64(latestdisk.(*bubt.Snapshot).Footprint())
	if (float64(snap.memheap()) / footprint) > bogn.flushratio {
		if nlevel := snap.nextbutlevel(latestlevel); nlevel >= 0 {
			return latestdisk, nlevel
		}
		return latestdisk, latestlevel
	}
	if latestlevel == 0 {
		panic("all levels are exhausted")
	}
	return nil, latestlevel - 1
}

func (bogn *Bogn) pickcompactdisks() (disks []api.Index, nextlevel int) {
	snap := bogn.currsnapshot()
	disks = snap.disklevels([]api.Index{})
	switch {
	case len(disks) == 1:
		// no compaction: there is only one disk level
		return nil, -1

	case len(disks) > 3:
		// aggressive compaction:
		// if number of levels is more than 3 then compact without
		// checking for compactratio or compactperiod.
		// leave the first level for flusher logic.
		return disks[1:], len(snap.disks) - 1
	}

	// normal compaction: use compactratio and compactperiod to two levels.
	for i := 0; i < len(disks)-1; i++ {
		disk0, disk1 := disks[i], disks[i+1]
		// check whether ratio between disk0 footprint and disk1 footprint
		// exceeds compactratio.
		footprint0 := float64(disk0.(*bubt.Snapshot).Footprint())
		footprint1 := float64(disk1.(*bubt.Snapshot).Footprint())
		ok1 := (footprint0 / footprint1) > bogn.compactratio
		// check whether disk0 lifetime exceeds compact period.
		metadata := bogn.diskmetadata(disk0)
		x, _ := strconv.Atoi(strings.Trim(metadata["flushunix"].(string), `"`))
		ok2 := time.Now().Sub(time.Unix(int64(x), 0)) > bogn.compactperiod
		// either of that is true, then
		if ok1 || ok2 {
			level1, _, _ := bogn.path2level(disk1.ID())
			if nextlevel = snap.nextbutlevel(level1); nextlevel >= 0 {
				return []api.Index{disk0, disk1}, nextlevel
			}
			return []api.Index{disk0, disk1}, level1
		}
	}
	return nil, -1
}

func (bogn *Bogn) levelname(level, version int, sha string) string {
	return fmt.Sprintf("%v-%v-%v-%v", bogn.name, level, version, sha)
}

func (bogn *Bogn) memlevelname(level string, version int) string {
	// level can be mw or mr or mc.
	return fmt.Sprintf("%v-%v-%v", bogn.name, level, version)
}

func (bogn *Bogn) logdir(logpath string) string {
	if len(logpath) == 0 && len(bogn.logpath) > 0 {
		logpath = bogn.logpath
	} else if len(bogn.logpath) == 0 {
		return ""
	}
	dirname := fmt.Sprintf("bogn-%v-logs", bogn.name)
	return filepath.Join(logpath, dirname)
}

func (bogn *Bogn) path2level(dirname string) (level, ver int, uuid string) {
	var err error

	parts := strings.Split(dirname, "-")
	if len(parts) == 4 && parts[0] == bogn.name {
		if level, err = strconv.Atoi(parts[1]); err != nil {
			return -1, -1, ""
		}
		if ver, err = strconv.Atoi(parts[2]); err != nil {
			return -1, -1, ""
		}
		return level, ver, parts[3]
	}
	return -1, -1, ""
}

func (bogn *Bogn) isclosed() bool {
	select {
	case <-bogn.finch:
		return true
	default:
	}
	return false
}

//---- Exported Control methods

// ID is same as the name of the instance used when creating it.
func (bogn *Bogn) ID() string {
	return bogn.name
}

func (bogn *Bogn) Getseqno() uint64 {
	return bogn.currsnapshot().mwseqno()
}

// BeginTxn starts a read-write transaction. All transactions should either
// be committed or aborted. If transactions are not released for long time
// it might increase the memory pressure on the system. Concurrent
// transactions are allowed, and serialized internally.
func (bogn *Bogn) BeginTxn(id uint64) api.Transactor {
	bogn.snaprlock()
	if snap := bogn.latestsnapshot(); snap != nil {
		txn := bogn.gettxn(id, bogn, snap)
		return txn
	}
	return nil
}

func (bogn *Bogn) commit(txn *Txn) (err error) {
	txn.snap.release()
	bogn.puttxn(txn)

	bogn.snaprunlock()
	return err
}

func (bogn *Bogn) aborttxn(txn *Txn) error {
	txn.snap.release()
	bogn.puttxn(txn)

	bogn.snaprunlock()
	return nil
}

// View starts a read-only transaction. Other than that it is similar
// to BeginTxn. All view transactions should be aborted.
func (bogn *Bogn) View(id uint64) api.Transactor {
	bogn.snaprlock()
	if snap := bogn.latestsnapshot(); snap != nil {
		view := bogn.getview(id, bogn, snap)
		return view
	}
	return nil
}

func (bogn *Bogn) abortview(view *View) error {
	view.snap.release()
	bogn.putview(view)

	bogn.snaprunlock()
	return nil
}

// Log vital statistics for all active bogn levels.
func (bogn *Bogn) Log() {
	bogn.snaprlock()
	defer bogn.snaprunlock()

	snap := bogn.latestsnapshot()
	if snap.mw != nil {
		bogn.logstore(snap.mw)
	}
	if snap.mr != nil {
		bogn.logstore(snap.mw)
	}
	if snap.mc != nil {
		bogn.logstore(snap.mw)
	}
	for _, disk := range snap.disklevels([]api.Index{}) {
		bogn.logstore(disk)
	}
	snap.release()
}

// Validate active bogn levels.
func (bogn *Bogn) Validate() {
	bogn.snaprlock()
	defer bogn.snaprunlock()

	// standard validation
	snap := bogn.latestsnapshot()
	disks := snap.disklevels([]api.Index{})
	seqno, endseqno := uint64(0), uint64(0)
	for i := len(disks) - 1; i >= 0; i-- {
		disk := disks[i]
		if disk != nil {
			endseqno = bogn.validatedisklevel(disk, seqno)
			fmsg := "%v validate disk %v (seqno %v to %v) ... ok"
			infof(fmsg, bogn.logprefix, disk.ID(), seqno, endseqno)
			seqno = endseqno
		}
	}

	if snap.mw != nil {
		bogn.validatestore(snap.mw)
	}
	if snap.mc != nil {
		bogn.validatestore(snap.mc)
	}

	snap.release()
}

// validate sort order.
// validate entries for seqno greater than minseqno.
// validate index count.
// validate index footprint.
func (bogn *Bogn) validatedisklevel(
	index api.Index, minseqno uint64) (maxseqno uint64) {

	var keymem, valmem, count int64
	var prevkey []byte

	var idxcount int64
	var idxfootprint int64
	var iter api.Iterator

	switch idx := index.(type) {
	case *bubt.Snapshot:
		iter = idx.Scan()
		idxcount, idxfootprint = idx.Count(), idx.Footprint()
	}

	key, val, seqno, _, err := iter(false /*fin*/)
	for err == nil {
		keymem, valmem = keymem+int64(len(key)), valmem+int64(len(val))
		if prevkey != nil {
			if bytes.Compare(prevkey, key) >= 0 {
				panic(fmt.Errorf("key %s comes before %s", prevkey, key))
			}
		}

		if seqno <= minseqno {
			fmsg := "entry %v seqno %v < minseqno %v"
			panic(fmt.Errorf(fmsg, key, seqno, minseqno))

		} else if seqno > maxseqno {
			maxseqno = seqno
		}
		count++
		prevkey = lib.Fixbuffer(prevkey, int64(len(key)))
		copy(prevkey, key)

		key, val, seqno, _, err = iter(false /*fin*/)
	}
	iter(true /*fin*/)
	if count != idxcount {
		panic(fmt.Errorf("expected %v entries, found %v", idxcount, count))
	}

	infof("%v found %v entries in %v", bogn.logprefix, count, index.ID())

	footprint := float64(keymem+valmem+(count*100)) * 1.5
	if idxfootprint > int64(footprint) {
		panic(fmt.Errorf("footprint %v exceeds %v", idxfootprint, footprint))
	}
	return maxseqno
}

// Close this instance, no calls allowed after Close.
func (bogn *Bogn) Close() {
	close(bogn.finch)

	for atomic.LoadInt64(&bogn.nroutines) > 0 {
		time.Sleep(10 * time.Millisecond)
	}

	// clear up all the snapshots.
	snap := bogn.currsnapshot()
	snap.addtopurge(snap.mw, snap.mr, snap.mc)
	disks := snap.disks
	for purgesnapshot(snap) == false {
		time.Sleep(10 * time.Millisecond)
		snap = bogn.currsnapshot()
	}
	for _, disk := range disks {
		if disk != nil {
			disk.Close()
		}
	}
	bogn.setheadsnapshot(nil)

	infof("%v closed ...", bogn.logprefix)
}

// Destroy the disk footprint of this instance, no calls allowed
// after Destroy.
func (bogn *Bogn) Destroy() {
	diskpaths := bogn.getdiskpaths()
	bogn.destroydisksnaps(bogn.logpath, bogn.diskstore, diskpaths)
	infof("%v destroyed ...", bogn.logprefix)
	return
}

//---- Exported read methods

// Get value for key, if value argument points to valid buffer it will, be
// used to copy the entry's value. Also returns entry's cas, whether entry
// is marked as deleted by LSM. If ok is false, then key is not found.
func (bogn *Bogn) Get(key, value []byte) (v []byte, cas uint64, del, ok bool) {
	snap := bogn.latestsnapshot()
	if snap.yget != nil {
		v, cas, del, ok = snap.yget(key, value)
	}
	snap.release()
	return
}

// Scan return a full table iterator, if iteration is stopped before
// reaching end of table (io.EOF), application should call iterator
// with fin as true. EG: iter(true)
func (bogn *Bogn) Scan() api.Iterator {
	var key, value []byte
	var seqno uint64
	var del bool
	var err error

	snap := bogn.latestsnapshot()
	iter := snap.iterator()
	return func(fin bool) ([]byte, []byte, uint64, bool, error) {
		if err == io.EOF {
			return nil, nil, 0, false, err

		} else if iter == nil {
			err = io.EOF
			snap.release()
			return nil, nil, 0, false, err

		} else if fin {
			iter(fin) // close all underlying iterations.
			err = io.EOF
			snap.release()
			return nil, nil, 0, false, err
		}
		if key, value, seqno, del, err = iter(fin); err == io.EOF {
			iter(fin)
			snap.release()
		}
		return key, value, seqno, del, err
	}
}

//---- Exported write methods

// Set a key, value pair in the index, if key is already present, its value
// will be over-written. Make sure key is not nil. Return old value if
// oldvalue points to valid buffer.
func (bogn *Bogn) Set(key, value, oldvalue []byte) (ov []byte, cas uint64) {
	bogn.snaprlock()
	ov, cas = bogn.currsnapshot().set(key, value, oldvalue)
	bogn.snaprunlock()
	return ov, cas
}

// SetCAS a key, value pair in the index, if CAS is ZERO then key should
// not be present in the index, otherwise existing CAS should match the
// supplied CAS. Value will be over-written. Make sure key is not nil.
// Return old value if oldvalue points to valid buffer.
func (bogn *Bogn) SetCAS(
	key, value, oldvalue []byte, cas uint64) ([]byte, uint64, error) {

	var ov []byte
	var rccas uint64
	var err error

	bogn.snaprlock()
	if atomic.LoadInt64(&bogn.dgmstate) == 0 {
		ov, rccas, err = bogn.currsnapshot().setCAS(key, value, oldvalue, cas)

	} else {
		txn := bogn.BeginTxn(0xABBA)
		_, gcas, deleted, ok := txn.Get(key, nil)
		ok1 := (ok && deleted == false) && gcas != cas
		ok2 := (ok == false || deleted) && cas != 0
		if ok1 || ok2 {
			return oldvalue, 0, api.ErrorInvalidCAS
		}
		ov = txn.Set(key, value, oldvalue)
		err = txn.Commit()
	}
	bogn.snaprunlock()
	return ov, rccas, err
}

// Delete key from index. Key should not be nil, if key found return its
// value. If lsm is true, then don't delete the node instead mark the node
// as deleted. Again, if lsm is true but key is not found in index, a new
// entry will inserted.
func (bogn *Bogn) Delete(key, oldvalue []byte, lsm bool) ([]byte, uint64) {
	bogn.snaprlock()
	if atomic.LoadInt64(&bogn.dgmstate) == 1 { // auto-enable lsm in dgm
		lsm = true
	}
	ov, cas := bogn.currsnapshot().delete(key, oldvalue, lsm)
	bogn.snaprunlock()
	return ov, cas
}

//---- local methods

func (bogn *Bogn) newmemstore(level string, seqno uint64) (api.Index, error) {
	var name string

	switch level {
	case "mw":
		bogn.memversions[0]++
		name = bogn.memlevelname(level, bogn.memversions[0])
	case "mc":
		bogn.memversions[2]++
		name = bogn.memlevelname(level, bogn.memversions[2])
	}

	switch bogn.memstore {
	case "llrb":
		llrbsetts := bogn.setts.Section("llrb.").Trim("llrb.")
		index := llrb.NewLLRB(name, llrbsetts)
		index.Setseqno(seqno)
		infof("%v new llrb store %q", bogn.logprefix, name)
		return index, nil

	case "mvcc":
		llrbsetts := bogn.setts.Section("llrb.").Trim("llrb.")
		index := llrb.NewMVCC(name, llrbsetts)
		index.Setseqno(seqno)
		infof("%v new mvcc store %q", bogn.logprefix, name)
		return index, nil
	}
	panic(fmt.Errorf("invalid memstore %q", bogn.memstore))
}

func (bogn *Bogn) builddiskstore(
	level, version int, sha, flushunix string, settstodisk s.Settings,
	iter api.Iterator) (index api.Index, err error) {

	switch bogn.diskstore {
	case "bubt":
		index, err = bogn.builddiskbubt(
			level, version, sha, flushunix, settstodisk, iter,
		)
		infof("%v new bubt snapshot %q", bogn.logprefix, index.ID())
		return
	}
	panic("impossible situation")
}

func (bogn *Bogn) builddiskbubt(
	level, version int, sha, flushunix string, settstodisk s.Settings,
	iter api.Iterator) (index api.Index, err error) {

	// book-keep largest seqno for this snapshot.
	var diskseqno, count uint64

	wrap := func(fin bool) ([]byte, []byte, uint64, bool, error) {
		if iter != nil {
			key, val, seqno, deleted, e := iter(fin)
			if seqno > diskseqno {
				diskseqno = seqno
			}
			if e == nil {
				count++
			}
			return key, val, seqno, deleted, e
		}
		return nil, nil, 0, false, io.EOF
	}

	now := time.Now()
	dirname := bogn.levelname(level, version, sha)

	bubtsetts := bogn.setts.Section("bubt.").Trim("bubt.")
	paths := bubtsetts.Strings("diskpaths")
	msize := bubtsetts.Int64("msize")
	zsize := bubtsetts.Int64("zsize")
	bt, err := bubt.NewBubt(dirname, paths, msize, zsize)
	if err != nil {
		errorf("%v NewBubt(): %v", bogn.logprefix, err)
		return nil, err
	}

	// build
	if err = bt.Build(wrap, nil); err != nil {
		errorf("%v Build(): %v", bogn.logprefix, err)
		return nil, err
	}
	mwmetadata := bogn.mwmetadata(diskseqno, flushunix, settstodisk)
	if _, err = bt.Writemetadata(mwmetadata); err != nil {
		errorf("%v Writemetadata(): %v", bogn.logprefix, err)
		return nil, err
	}
	bt.Close()

	// TODO: make this as separate function and let it be called
	// with more customization in dopersist, doflush, findisk, dowindup.
	mmap := bubtsetts.Bool("mmap")
	snap := bogn.currsnapshot()
	if mmap == false && snap != nil {
		if latestlevel, _ := snap.latestlevel(); level <= latestlevel {
			mmap = true
		}
	}
	ndisk, err := bubt.OpenSnapshot(dirname, paths, mmap)
	if err != nil {
		errorf("%v OpenSnapshot(): %v", bogn.logprefix, err)
		return nil, err
	}

	footprint := humanize.Bytes(uint64(ndisk.Footprint()))
	elapsed := time.Since(now)
	fmsg := "%v took %v to build bubt %v with %v entries, %v\n"
	infof(fmsg, bogn.logprefix, elapsed, ndisk.ID(), count, footprint)

	return ndisk, nil
}

// open latest versions for each disk level
func (bogn *Bogn) opendisksnaps(
	setts s.Settings) (disks [16]api.Index, err error) {

	switch bogn.diskstore {
	case "bubt":
		bubtsetts := bogn.setts.Section("bubt.").Trim("bubt.")
		diskpaths := bubtsetts.Strings("diskpaths")
		mmap := bubtsetts.Bool("mmap")
		disks, err = bogn.openbubtsnaps(diskpaths, mmap)

	default:
		panic("impossible situation")
	}

	if err != nil {
		return disks, err
	}

	// log information about active disksnapshots.
	n := 0
	for _, disk := range disks {
		if disk == nil {
			continue
		}
		n++
		infof("%v open-disksnapshot %v", bogn.logprefix, disk.ID())
	}
	if n > 1 {
		bogn.dgmstate = 1
	}
	return disks, nil
}

// open latest versions for each disk level from bubt snapshots.
func (bogn *Bogn) openbubtsnaps(
	paths []string, mmap bool) ([16]api.Index, error) {

	var disks [16]api.Index

	dircache := map[string]bool{}
	for _, path := range paths {
		fis, err := ioutil.ReadDir(path)
		if err != nil {
			errorf("%v ReadDir(%q): %v", bogn.logprefix, path, err)
			return disks, err
		}
		for _, fi := range fis {
			if !fi.IsDir() {
				continue
			}
			dirname := fi.Name()
			if _, ok := dircache[dirname]; ok {
				continue
			}
			level, _, _ := bogn.path2level(dirname)
			if level < 0 {
				continue // not a bogn disk level
			}
			disk, err := bubt.OpenSnapshot(dirname, paths, mmap)
			if err != nil {
				return disks, err
			}
			if disks[level] != nil {
				panic("impossible situation")
			}
			disks[level] = disk
			dircache[dirname] = true
		}
	}
	return disks, nil
}

// compact away older versions in disk levels.
func (bogn *Bogn) compactdisksnaps(
	diskstore string, diskpaths []string, merge bool) error {

	switch diskstore {
	case "bubt":
		return bogn.compactbubtsnaps(diskpaths, merge)
	}
	panic(fmt.Errorf("invalid diskstore %v", diskstore))
}

func (bogn *Bogn) compactbubtsnaps(diskpaths []string, merge bool) error {
	var disks [16]api.Index

	mmap, dircache := false, map[string]bool{}
	for _, path := range diskpaths {
		fis, err := ioutil.ReadDir(path)
		if err != nil {
			errorf("%v ReadDir(%q): %v", bogn.logprefix, path, err)
			return err
		}
		for _, fi := range fis {
			if !fi.IsDir() {
				continue
			}
			dirname := fi.Name()
			if _, ok := dircache[dirname]; ok {
				continue
			}
			level, version, _ := bogn.path2level(dirname)
			if level < 0 {
				continue // not a bogn directory
			}
			disk, err := bubt.OpenSnapshot(dirname, diskpaths, mmap)
			if err != nil { // bad snapshot
				bubt.PurgeSnapshot(dirname, diskpaths)
				continue
			}
			if od := disks[level]; od == nil { // first version
				disks[level] = disk

			} else if _, over, _ := bogn.path2level(od.ID()); over < version {
				fmsg := "%v compact away older version %v"
				infof(fmsg, bogn.logprefix, od.ID())
				bogn.destroylevels(od)
				disks[level] = disk

			} else {
				fmsg := "%v compact away older version %v"
				infof(fmsg, bogn.logprefix, disk.ID())
				bogn.destroylevels(disk)
			}
			dircache[dirname] = true
		}
	}

	validdisks := []api.Index{}
	for _, disk := range disks {
		if disk != nil {
			validdisks = append(validdisks, disk)
		}
	}
	if len(validdisks) == 0 {
		infof("%v no disk levels found for compaction", bogn.logprefix)
	} else if merge {
		return bogn.mergedisksnapshots(validdisks)
	}
	bogn.closelevels(validdisks...)
	return nil
}

func (bogn *Bogn) mergedisksnapshots(disks []api.Index) error {
	scans := make([]api.Iterator, 0)
	sourceids := []string{}
	for _, disk := range disks {
		if iter := disk.Scan(); iter != nil {
			scans = append(scans, iter)
		}
		sourceids = append(sourceids, disk.ID())
	}

	// get latest settings, from latest disk, including memversions and
	// diskversions and use them when building the merged snapshot.
	disksetts := bogn.settingsfromdisk(disks[0])
	flushunix := bogn.getflushunix(disks[0])
	bogn.setts = disksetts

	infof("%v merging [%v]", bogn.logprefix, strings.Join(sourceids, ","))

	iter := reduceiter(scans)
	level, uuid := 15, bogn.newuuid()
	diskversions := bogn.getdiskversions(disks[0])
	version := diskversions[level] + 1
	ndisk, err := bogn.builddiskstore(
		level, version, uuid, flushunix, disksetts, iter,
	)
	if err != nil {
		return err
	}
	iter(true /*fin*/)

	ndisk.Close()

	for _, disk := range disks[:] {
		if disk != nil {
			infof("%v merged out %q", bogn.logprefix, disk.ID())
			disk.Close()
			disk.Destroy()
		}
	}
	return nil
}

func (bogn *Bogn) destroydisksnaps(
	logpath, diskstore string, diskpaths []string) error {

	// purge log dir
	paths := make([]string, len(diskpaths))
	copy(paths, diskpaths)
	if len(logpath) > 0 {
		paths = append(paths, logpath)
	}
	if err := bogn.destorybognlogs(paths); err != nil {
		return err
	}

	// purge disk snapshots
	switch diskstore {
	case "bubt":
		if err := bogn.destroybubtsnaps(diskpaths); err != nil {
			return err
		}
		return nil
	}
	panic("unreachable code")
}

func (bogn *Bogn) destorybognlogs(diskpaths []string) error {
	for _, path := range diskpaths {
		logdir := bogn.logdir(path)
		if fi, err := os.Stat(logdir); err != nil {
			continue

		} else if fi.IsDir() {
			if err := os.RemoveAll(logdir); err != nil {
				errorf("%v RemoveAll(%q): %v", bogn.logprefix, logdir, err)
				return err
			}
			infof("%v removed logdir %q", bogn.logprefix, logdir)
			return nil
		}
	}
	infof("%v no logdir found !!", bogn.logprefix)
	return nil
}

func (bogn *Bogn) destroybubtsnaps(diskpaths []string) error {
	pathlist := strings.Join(diskpaths, ", ")
	for _, path := range diskpaths {
		fis, err := ioutil.ReadDir(path)
		if err != nil {
			errorf("%v ReadDir(%q): %v", bogn.logprefix, path, err)
			return err
		}
		for _, fi := range fis {
			if !fi.IsDir() {
				continue
			}
			level, _, _ := bogn.path2level(fi.Name())
			if level < 0 {
				continue // not a bogn directory
			}
			fmsg := "%v purge bubt snapshot %q under %q"
			infof(fmsg, bogn.logprefix, fi.Name(), pathlist)
			bubt.PurgeSnapshot(fi.Name(), diskpaths)
		}
	}
	return nil
}

// release resources held in disk levels.
func (bogn *Bogn) closelevels(indexes ...api.Index) {
	for _, index := range indexes {
		if index != nil {
			index.Close()
		}
	}
}

// destroy disk footprint in disk levels.
func (bogn *Bogn) destroylevels(indexes ...api.Index) {
	for _, index := range indexes {
		if index != nil {
			index.Close()
			index.Destroy()
		}
	}
}

func (bogn *Bogn) currdiskversion(level int) int {
	return bogn.diskversions[level]
}

func (bogn *Bogn) nextdiskversion(level int) int {
	bogn.diskversions[level]++
	return bogn.diskversions[level]
}

func (bogn *Bogn) logstore(index api.Index) {
	switch idx := index.(type) {
	case *llrb.LLRB:
		idx.Log()
	case *llrb.MVCC:
		idx.Log()
	case *bubt.Snapshot:
		idx.Log()
	}
}

func (bogn *Bogn) validatestore(index api.Index) {
	switch idx := index.(type) {
	case *llrb.LLRB:
		idx.Validate()
	case *llrb.MVCC:
		idx.Validate()
	}
}

func (bogn *Bogn) getdiskpaths() []string {
	switch bogn.diskstore {
	case "bubt":
		bubtsetts := bogn.setts.Section("bubt.").Trim("bubt.")
		return bubtsetts.Strings("diskpaths")
	}
	panic("impossible situation")
}

func (bogn *Bogn) newuuid() string {
	uuid, err := lib.Newuuid(make([]byte, 8))
	if err != nil {
		panic(err)
	}
	uuidb := make([]byte, 16)
	return string(uuidb[:uuid.Format(uuidb)])
}

func (bogn *Bogn) getdiskseqno(disk api.Index) uint64 {
	metadata := bogn.diskmetadata(disk)
	return metadata["seqno"].(uint64)
}

func (bogn *Bogn) getflushunix(disk api.Index) string {
	metadata := bogn.diskmetadata(disk)
	return metadata["flushunix"].(string)
}

func (bogn *Bogn) getdiskversions(disk api.Index) [16]int {
	metadata := bogn.diskmetadata(disk)
	return metadata["diskversions"].([16]int)
}

func (bogn *Bogn) diskmetadata(disk api.Index) map[string]interface{} {
	metadata := make(map[string]interface{})

	switch d := disk.(type) {
	case *bubt.Snapshot:
		err := json.Unmarshal(d.Metadata(), &metadata)
		if err != nil {
			panic(err)
		}

		setts := s.Settings(metadata).Section("bogn.").Trim("bogn.")
		metadata = map[string]interface{}(setts)

		// cure `seqno`
		val := metadata["seqno"].(string)
		seqno, err := strconv.ParseUint(strings.Trim(val, `"`), 10, 64)
		if err != nil {
			panic(err)
		}
		metadata["seqno"] = seqno

		// cure memversions
		mvers := metadata["memversions"].([]interface{})
		metadata["memversions"] = [3]int{
			int(mvers[0].(float64)),
			int(mvers[1].(float64)),
			int(mvers[2].(float64)),
		}

		// cure diskversions
		diskversions := [16]int{}
		for i, v := range metadata["diskversions"].([]interface{}) {
			diskversions[i] = int(v.(float64))
		}
		metadata["diskversions"] = diskversions

		return metadata
	}
	panic("unreachable code")
}
