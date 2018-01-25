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
import "io/ioutil"
import "sync/atomic"
import "encoding/json"

import "github.com/prataprc/gostore/api"
import "github.com/prataprc/gostore/lib"
import "github.com/prataprc/gostore/llrb"
import "github.com/prataprc/gostore/bubt"
import "github.com/bnclabs/golog"
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

	name     string
	epoch    time.Time
	snapshot unsafe.Pointer // *snapshot
	mwver    int
	mcver    int
	finch    chan struct{}
	snaprw   sync.RWMutex
	txnmeta

	// bogn settings
	memstore    string
	diskstore   string
	merge       bool
	durable     bool
	dgm         bool
	workingset  bool
	ratio       float64
	period      time.Duration
	memcapacity int64
	setts       s.Settings
	logprefix   string
}

// New create a new bogn instance.
func New(name string, setts s.Settings) (*Bogn, error) {
	bogn := (&Bogn{name: name}).readsettings(setts)
	bogn.inittxns()
	bogn.epoch = time.Now()
	if err := bogn.makepaths(setts); err != nil {
		return nil, err
	}
	bogn.finch = make(chan struct{})
	bogn.logprefix = fmt.Sprintf("BOGN [%v]", name)

	log.Infof("%v starting ...", bogn.logprefix)

	bogn.Compact(bogn.merge)

	disks, err := bogn.opendisksnaps(setts)
	if err != nil {
		bogn.Close()
		return nil, err
	}
	// NOTE: If settings have changed in between a re-boot from disk,
	// a migration tool needs to be applied to move disk snapshots
	// from older settings to new settings.
	bogn.settingsfromdisks(disks[:])

	mw := bogn.warmupfromdisk(disks[:])

	head, err := opensnapshot(bogn, mw, disks)
	if err != nil {
		bogn.Close()
		return nil, err
	}
	head.refer()
	bogn.setheadsnapshot(head)

	return bogn, nil
}

// IMPORTANT: when ever this functin is updated, please update
// settingsfromdisk(), settingsfromdisks(), settingstodisk() and
// validatesettings().
func (bogn *Bogn) readsettings(setts s.Settings) *Bogn {
	bogn.memstore = setts.String("memstore")
	bogn.diskstore = setts.String("diskstore")
	bogn.merge = setts.Bool("merge")
	bogn.durable = setts.Bool("durable")
	bogn.dgm = setts.Bool("dgm")
	bogn.workingset = setts.Bool("workingset")
	bogn.ratio = setts.Float64("ratio")
	bogn.period = time.Duration(setts.Int64("period")) * time.Second
	bogn.setts = setts

	atomic.StoreInt64(&bogn.dgmstate, 0)
	if bogn.dgm {
		atomic.StoreInt64(&bogn.dgmstate, 1)
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
	setts := s.Settings{
		"memstore":   bogn.memstore,
		"diskstore":  bogn.diskstore,
		"workingset": bogn.workingset,
		"ratio":      bogn.ratio,
		"period":     bogn.period,
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
func (bogn *Bogn) settingsfromdisks(disks []api.Index) {
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
		bogn.validatesettings(alldisks[0], disks)
	}
}

func (bogn *Bogn) validatesettings(ndisk api.Index, disks []api.Index) {
	setts, nsetts := bogn.setts, bogn.settingsfromdisk(ndisk)
	if diskstore := nsetts.String("diskstore"); diskstore != bogn.diskstore {
		fmsg := "found diskstore:%q on disk, expected %q"
		panic(fmt.Errorf(fmsg, diskstore, bogn.diskstore))
	}
	// bubt settings
	diskpaths1 := nsetts.Strings("bubt.diskpaths")
	sort.Strings(diskpaths1)
	diskpaths2 := setts.Strings("bubt.diskpaths")
	sort.Strings(diskpaths2)
	if reflect.DeepEqual(diskpaths1, diskpaths2) == false {
		fmsg := "found diskpaths:%v on disk, expected %v"
		panic(fmt.Errorf(fmsg, diskpaths1, diskpaths2))
	}
	msize1, msize2 := nsetts.Int64("bubt.msize"), setts.Int64("bubt.msize")
	if msize1 != msize2 {
		fmsg := "found msize:%v on disk, expected %v"
		panic(fmt.Errorf(fmsg, msize1, msize2))
	}
	zsize1, zsize2 := nsetts.Int64("bubt.zsize"), setts.Int64("bubt.zsize")
	if zsize1 != zsize2 {
		fmsg := "found zsize:%v on disk, expected %v"
		panic(fmt.Errorf(fmsg, zsize1, zsize2))
	}
	mmap1, mmap2 := nsetts.Bool("bubt.mmap"), nsetts.Bool("bubt.mmap")
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
	checkcapacity := func(memcapacity, count, footprint int64) bool {
		nodesize := int64(unsafe.Sizeof(llrb.Llrbnode{})) - 8
		expected := ((footprint / count) + (nodesize * 2)) * count
		expected = int64(float64(expected) * float64(1.2)) // TODO: no magic
		if expected < memcapacity {
			return true
		}
		bogn.dgmstate = 1
		return false
	}

	var ndisk api.Index

	for _, ndisk = range disks {
		if ndisk == nil {
			continue
		}
		break
	}
	if ndisk == nil {
		return nil
	}

	var entries, footprint int64

	switch bogn.diskstore {
	case "bubt":
		entries = ndisk.(*bubt.Snapshot).Count()
		footprint = ndisk.(*bubt.Snapshot).Footprint()
	}

	switch bogn.memstore {
	case "llrb":
		llrbsetts := bogn.setts.Section("llrb.").Trim("llrb.")
		memcapacity := llrbsetts.Int64("memcapacity")
		if checkcapacity(memcapacity, entries, footprint) {
			return bogn.llrbfromdisk(ndisk, entries, footprint)
		}

	case "mvcc":
		llrbsetts := bogn.setts.Section("llrb.").Trim("llrb.")
		memcapacity := llrbsetts.Int64("memcapacity")
		if checkcapacity(memcapacity, entries, footprint) {
			return bogn.mvccfromdisk(ndisk, entries, footprint)
		}

	default:
		panic("unreachable code")
	}

	fmsg := "%v memory capacity too small to warmup %v, %v entries"
	arg1 := humanize.Bytes(uint64(footprint))
	log.Infof(fmsg, bogn.logprefix, arg1, entries)
	return nil
}

func (bogn *Bogn) llrbfromdisk(
	ndisk api.Index, entries, footprint int64) api.Index {

	now := time.Now()

	iter, seqno := ndisk.Scan(), bogn.diskseqno(ndisk)
	name := fmt.Sprintf("%v-%v-%v", bogn.name, "mw", bogn.mwver)
	llrbsetts := bogn.setts.Section("llrb.").Trim("llrb.")
	mw := llrb.LoadLLRB(name, llrbsetts, iter)
	mw.Setseqno(seqno)
	iter(true /*fin*/)

	fmsg := "%v warmup LLRB %v (%v) %v entries -> %v in %v"
	arg1 := humanize.Bytes(uint64(footprint))
	log.Infof(
		fmsg, bogn.logprefix, ndisk.ID(), arg1, entries, mw.ID(),
		time.Since(now).Round(time.Second),
	)

	return mw
}

func (bogn *Bogn) mvccfromdisk(
	ndisk api.Index, entries, footprint int64) api.Index {

	now := time.Now()

	iter, seqno := ndisk.Scan(), bogn.diskseqno(ndisk)
	name := fmt.Sprintf("%v-%v-%v", bogn.name, "mw", bogn.mwver)
	llrbsetts := bogn.setts.Section("llrb.").Trim("llrb.")
	mw := llrb.LoadMVCC(name, llrbsetts, iter)
	mw.Setseqno(seqno)
	iter(true /*fin*/)

	fmsg := "%v warmup MVCC %v (%v) %v entries -> %v in %v"
	arg1 := humanize.Bytes(uint64(footprint))
	log.Infof(
		fmsg, bogn.logprefix, ndisk.ID(), arg1, entries, mw.ID(),
		time.Since(now).Round(time.Second),
	)

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
	bubtsetts := setts.Section("bubt.").Trim("bubt.")
	for _, path := range bubtsetts.Strings("diskpaths") {
		if err := os.MkdirAll(path, 0775); err != nil {
			log.Errorf("%v %v", bogn.logprefix, err)
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

func (bogn *Bogn) mwmetadata(seqno uint64) []byte {
	metadata := map[string]interface{}{
		"seqno":     fmt.Sprintf(`"%v"`, seqno),
		"flushunix": fmt.Sprintf(`"%v"`, time.Now().Unix()),
	}
	setts := (s.Settings{}).Mixin(metadata, bogn.settingstodisk())
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
		return int64(time.Since(bogn.epoch)) > int64(bogn.period)
	}

	var metadata map[string]interface{}
	switch d := disk.(type) {
	case *bubt.Snapshot:
		metadata = bogn.diskmetadata(d)
	default:
		panic("impossible situation")
	}
	x, _ := strconv.Atoi(strings.Trim(metadata["flushunix"].(string), `"`))
	return time.Now().Sub(time.Unix(int64(x), 0)) > bogn.period
}

// should not overlap with disk0.
func (bogn *Bogn) pickflushdisk(disk0 api.Index) (api.Index, int, int) {
	snap := bogn.currsnapshot()

	latestlevel, latestdisk := snap.latestlevel()
	if latestlevel < 0 && disk0 != nil {
		panic("impossible situation")

	} else if latestlevel < 0 { // first time flush.
		return nil, len(snap.disks) - 1, 1

	} else if latestdisk != nil && disk0 != nil {
		level0, _, _ := bogn.path2level(disk0.ID())
		if level0 < latestlevel {
			panic("impossible situation")
		} else if level0 == latestlevel { // fall back by one level.
			if latestlevel == 0 {
				panic("all levels are exhausted")
			}
			return nil, latestlevel - 1, 1
		}
	}
	_, version, _ := bogn.path2level(latestdisk.ID())
	footprint := float64(latestdisk.(*bubt.Snapshot).Footprint())
	if (float64(snap.memheap()) / footprint) > bogn.ratio {
		if latestlevel == 0 {
			panic("all levels are exhausted")
		}
		return latestdisk, latestlevel - 1, 1
	}
	return latestdisk, latestlevel, version + 1
}

func (bogn *Bogn) pickcompactdisks() (disk0, disk1 api.Index, nextlevel int) {
	snap := bogn.currsnapshot()
	disks := snap.disklevels([]api.Index{})
	for i := 0; i < len(disks)-1; i++ {
		disk0, disk1 = disks[i], disks[i+1]
		footprint0 := float64(disk0.(*bubt.Snapshot).Footprint())
		footprint1 := float64(disk1.(*bubt.Snapshot).Footprint())
		if (footprint0 / footprint1) < bogn.ratio {
			continue
		}
		level1, _, _ := bogn.path2level(disk1.ID())
		if nextlevel = snap.nextemptylevel(level1); nextlevel < 0 {
			level0, _, _ := bogn.path2level(disk0.ID())
			if nextlevel = snap.nextemptylevel(level0); nextlevel < 0 {
				nextlevel = level1
			}
		}
		return disk0, disk1, nextlevel
	}
	return nil, nil, -1
}

func (bogn *Bogn) levelname(level, version int, sha string) string {
	return fmt.Sprintf("%v-%v-%v-%v", bogn.name, level, version, sha)
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

// Compact away oldver version of disk snapshots. If merge is true
// then merge all levels into one big level.
func (bogn *Bogn) Compact(merge bool) error {
	return bogn.compactdisksnaps(merge, bogn.setts)
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
	disks, seqno := snap.disklevels([]api.Index{}), uint64(0)
	for i := len(disks) - 1; i >= 0; i-- {
		if disk := disks[i]; disk != nil {
			seqno = bogn.validatedisklevel(disk, seqno)
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
	footprint := (float64(keymem) * 1.5) + (float64(valmem) * 1.5)
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

	log.Infof("%v closed ...", bogn.logprefix)
}

// Destroy the disk footprint of this instance, no calls allowed
// after Destroy.
func (bogn *Bogn) Destroy() {
	bogn.destroydisksnaps(bogn.setts)
	return
}

func PurgeIndex(name string, setts s.Settings) {
	bogn := (&Bogn{name: name}).readsettings(setts)
	bogn.logprefix = fmt.Sprintf("BOGN [%v]", name)
	bogn.destroydisksnaps(setts)
	return
}

//---- Exported read methods

// Get value for key, if value argument points to valid buffer it will, be
// used to copy the entry's value. Also returns entry's cas, whether entry
// is marked as deleted by LSM. If ok is false, then key is not found.
func (bogn *Bogn) Get(
	key, value []byte) (v []byte, cas uint64, deleted, ok bool) {

	snap := bogn.latestsnapshot()
	if snap.yget != nil {
		v, cas, deleted, ok = snap.yget(key, value)
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
			return nil, nil, 0, false, err

		} else if fin {
			iter(fin) // close all underlying iterations.
			err = io.EOF
			snap.release()
			return nil, nil, 0, false, err
		}
		if key, value, seqno, del, err = iter(fin); err == io.EOF {
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
		name = fmt.Sprintf("%v-%v-%v", bogn.name, level, bogn.mwver)
		bogn.mwver++
	case "mc":
		name = fmt.Sprintf("%v-%v-%v", bogn.name, level, bogn.mcver)
		bogn.mcver++
	}

	switch bogn.memstore {
	case "llrb":
		llrbsetts := bogn.setts.Section("llrb.").Trim("llrb.")
		index := llrb.NewLLRB(name, llrbsetts)
		index.Setseqno(seqno)
		return index, nil

	case "mvcc":
		llrbsetts := bogn.setts.Section("llrb.").Trim("llrb.")
		index := llrb.NewMVCC(name, llrbsetts)
		index.Setseqno(seqno)
		return index, nil
	}
	panic(fmt.Errorf("invalid memstore %q", bogn.memstore))
}

func (bogn *Bogn) builddiskstore(
	level, version int, sha string,
	iter api.Iterator) (index api.Index, err error) {

	// TODO: use diskstore for configurable persistance algorithm.

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
		log.Errorf("%v NewBubt(): %v", bogn.logprefix, err)
		return nil, err
	}

	// build
	if err = bt.Build(wrap, nil); err != nil {
		log.Errorf("%v Build(): %v", bogn.logprefix, err)
		return nil, err
	}
	if _, err = bt.Writemetadata(bogn.mwmetadata(diskseqno)); err != nil {
		log.Errorf("%v Writemetadata(): %v", bogn.logprefix, err)
		return nil, err
	}
	bt.Close()

	// open disk
	mmap := bubtsetts.Bool("mmap")
	ndisk, err := bubt.OpenSnapshot(dirname, paths, mmap)
	if err != nil {
		log.Errorf("%v OpenSnapshot(): %v", bogn.logprefix, err)
		return nil, err
	}

	footprint := humanize.Bytes(uint64(ndisk.Footprint()))
	elapsed := time.Since(now)
	fmsg := "%v took %v to build %v with %v entries, %v\n"
	log.Infof(fmsg, bogn.logprefix, elapsed, ndisk.ID(), count, footprint)

	return ndisk, nil
}

// open latest versions for each disk level
func (bogn *Bogn) opendisksnaps(
	setts s.Settings) (disks [16]api.Index, err error) {

	switch bogn.diskstore {
	case "bubt":
		bubtsetts := bogn.setts.Section("bubt.").Trim("bubt.")
		paths := bubtsetts.Strings("diskpaths")
		mmap := bubtsetts.Bool("mmap")
		disks, err = bogn.openbubtsnaps(paths, mmap)

	default:
		panic("unreachable code")
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
		log.Infof("%v open-disksnapshot %v", bogn.logprefix, disk.ID())
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
			log.Errorf("%v %v", bogn.logprefix, err)
			return disks, err
		}
		for _, fi := range fis {
			dirname := fi.Name()
			if _, ok := dircache[dirname]; ok {
				continue
			}
			level, version, _ := bogn.path2level(dirname)
			if level < 0 {
				continue // not a bogn disk level
			}

			disk, err := bubt.OpenSnapshot(dirname, paths, mmap)
			if err != nil {
				return disks, err
			}

			if od := disks[level]; od == nil { // first version
				disks[level] = disk

			} else if _, over, _ := bogn.path2level(od.ID()); over < version {
				bogn.closelevels(od)
				disks[level] = disk

			} else {
				bogn.closelevels(disk)
			}
			dircache[dirname] = true
		}
	}
	return disks, nil
}

// compact away older versions in disk levels.
func (bogn *Bogn) compactdisksnaps(merge bool, setts s.Settings) (err error) {
	var disks [16]api.Index

	bubtsetts := bogn.setts.Section("bubt.").Trim("bubt.")
	paths := bubtsetts.Strings("diskpaths")
	mmap := bubtsetts.Bool("mmap")

	dircache := map[string]bool{}
	for _, path := range paths {
		fis, err := ioutil.ReadDir(path)
		if err != nil {
			log.Errorf("%v %v", bogn.logprefix, err)
			return err
		}
		for _, fi := range fis {
			dirname := fi.Name()
			if _, ok := dircache[dirname]; ok {
				continue
			}
			level, version, _ := bogn.path2level(dirname)
			if level < 0 {
				continue // not a bogn directory
			}
			disk, err := bubt.OpenSnapshot(dirname, paths, mmap)
			if err != nil { // bad snapshot
				bubt.PurgeSnapshot(dirname, paths)
				continue
			}
			if od := disks[level]; od == nil { // first version
				disks[level] = disk

			} else if _, over, _ := bogn.path2level(od.ID()); over < version {
				bogn.destroylevels(disks[level])
				disks[level] = disk

			} else {
				bogn.destroylevels(disk)
			}
			dircache[dirname] = true
		}
	}

	if merge {
		// disks will closed and destroyed, a new disk snapshot will be
		// created.
		err = bogn.mergedisksnapshots(disks[:])
	} else {
		bogn.closelevels(disks[:]...)
	}
	return err
}

func (bogn *Bogn) mergedisksnapshots(disks []api.Index) error {
	var olddisk api.Index

	scans := make([]api.Iterator, 0)
	arg1 := []string{}
	for _, disk := range disks {
		if disk == nil {
			continue
		}
		olddisk = disk
		if iter := disk.Scan(); iter != nil {
			scans = append(scans, iter)
		}
		arg1 = append(arg1, disk.ID())
	}

	if len(scans) > 0 {
		iter := reduceiter(scans)
		_, version, _ := bogn.path2level(olddisk.ID())
		nlevel, nversion, uuid := len(disks)-1, version+1, bogn.newuuid()
		ndisk, err := bogn.builddiskstore(nlevel, nversion, uuid, iter)
		if err != nil {
			panic(err)
		}
		iter(true /*fin*/)
		fmsg := "%v merging [%v] -> %v"
		log.Infof(fmsg, bogn.logprefix, strings.Join(arg1, ","), ndisk.ID())
		ndisk.Close()
	}

	for _, disk := range disks[:] {
		if disk != nil {
			disk.Close()
			disk.Destroy()
		}
	}
	return nil
}

func (bogn *Bogn) destroydisksnaps(bognsetts s.Settings) error {
	switch bognsetts.String("diskstore") {
	case "bubt":
		bubtsetts := bognsetts.Section("bubt.").Trim("bubt.")
		return bogn.destroybubtsnaps(bubtsetts)
	}
	panic("unreachable code")
}

func (bogn *Bogn) destroybubtsnaps(bubtsetts s.Settings) error {
	paths := bubtsetts.Strings("diskpaths")
	for _, path := range paths {
		fis, err := ioutil.ReadDir(path)
		if err != nil {
			log.Errorf("%v %v", bogn.logprefix, err)
			return err
		}
		for _, fi := range fis {
			level, _, _ := bogn.path2level(fi.Name())
			if level < 0 {
				continue // not a bogn directory
			}
			bubt.PurgeSnapshot(fi.Name(), paths)
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

func (bogn *Bogn) newuuid() string {
	uuid, err := lib.Newuuid(make([]byte, 8))
	if err != nil {
		panic(err)
	}
	uuidb := make([]byte, 16)
	return string(uuidb[:uuid.Format(uuidb)])
}

func (bogn *Bogn) diskseqno(disk api.Index) uint64 {
	metadata := bogn.diskmetadata(disk)
	sseqno := metadata["seqno"].(string)
	seqno, err := strconv.ParseUint(strings.Trim(sseqno, `"`), 10, 64)
	if err != nil {
		panic(err)
	}
	return seqno
}

func (bogn *Bogn) diskcount(disk api.Index) int64 {
	metadata := bogn.diskmetadata(disk)
	return int64(metadata["ncount"].(float64))
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
		return map[string]interface{}(setts)
	}
	panic("unreachable code")
}
