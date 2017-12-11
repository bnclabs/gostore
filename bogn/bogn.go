package bogn

import "io"
import "os"
import "fmt"
import "sync"
import "unsafe"
import "time"
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
import "github.com/prataprc/golog"
import s "github.com/prataprc/gosettings"
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

	memstore    string
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

	disks, err := bogn.opendisksnaps(setts)
	if err != nil {
		bogn.Close()
		return nil, err
	}
	head, err := opensnapshot(bogn, disks)
	if err != nil {
		bogn.Close()
		return nil, err
	}
	head.refer()
	bogn.setheadsnapshot(head)

	log.Infof("%v started", bogn.logprefix)

	return bogn, nil
}

func (bogn *Bogn) Start() *Bogn {
	go purger(bogn)
	go compactor(bogn)
	return bogn
}

func (bogn *Bogn) readsettings(setts s.Settings) *Bogn {
	bogn.memstore = setts.String("memstore")
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

	switch bogn.memstore {
	case "llrb", "mvcc":
		llrbsetts := bogn.setts.Section("llrb.").Trim("llrb.")
		bogn.memcapacity = llrbsetts.Int64("memcapacity")
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
	data, err := json.Marshal(metadata)
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

	var metadata []byte
	switch index := disk.(type) {
	case *bubt.Snapshot:
		metadata = index.Metadata()
	default:
		panic("impossible situation")
	}

	var md map[string]interface{}
	if err := json.Unmarshal(metadata, &md); err != nil {
		panic(err)
	}
	x, _ := strconv.Atoi(strings.Trim(md["flushunix"].(string), `"`))
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

// Compact away oldver version of disk snapshots.
func (bogn *Bogn) Compact() {
	bogn.compactdisksnaps(bogn.setts)
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

	snap := bogn.latestsnapshot()
	if snap.mw != nil {
		bogn.validatestore(snap.mw)
	}
	if snap.mc != nil {
		bogn.validatestore(snap.mc)
	}
	for _, disk := range snap.disklevels([]api.Index{}) {
		bogn.validatestore(disk)
	}
	snap.release()
}

// Close this instance, no calls allowed after Close.
func (bogn *Bogn) Close() {
	close(bogn.finch)
	for atomic.LoadInt64(&bogn.nroutines) > 0 {
		time.Sleep(100 * time.Millisecond)
	}
	bogn.setheadsnapshot(nil)
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

// Scan return a full table iterator.
func (bogn *Bogn) Scan() api.Iterator {
	var err error
	snap := bogn.latestsnapshot()
	iter := snap.iterator()
	return func(fin bool) (key, val []byte, seqno uint64, del bool, e error) {
		if err == io.EOF || iter == nil {
			return nil, nil, 0, false, io.EOF

		} else if fin == false {
			key, val, seqno, del, e = iter(fin)
			if err = e; err == io.EOF {
				snap.release()
			}
			return
		}
		err = io.EOF
		snap.release()
		return
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

	bogn.snaprlock()
	ov, cas, err := bogn.currsnapshot().setCAS(key, value, oldvalue, cas)
	bogn.snaprunlock()
	return ov, cas, err
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
	} else if err = bt.Writemetadata(bogn.mwmetadata(diskseqno)); err != nil {
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
func (bogn *Bogn) opendisksnaps(setts s.Settings) ([16]api.Index, error) {
	var disks [16]api.Index

	bubtsetts := bogn.setts.Section("bubt.").Trim("bubt.")
	paths := bubtsetts.Strings("diskpaths")
	mmap := bubtsetts.Bool("mmap")

	for _, path := range paths {
		fis, err := ioutil.ReadDir(path)
		if err != nil {
			log.Errorf("%v %v", bogn.logprefix, err)
			return disks, err
		}
		for _, fi := range fis {
			dirname := fi.Name()
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
		}
	}

	// log information about active disksnapshots.
	for _, disk := range disks {
		if disk == nil {
			continue
		}
		log.Infof("%v open-disksnapshot %v", bogn.logprefix, disk.ID())
	}
	return disks, nil
}

// compact away older versions in disk levels.
func (bogn *Bogn) compactdisksnaps(setts s.Settings) {
	var disks [16]api.Index

	bubtsetts := bogn.setts.Section("bubt.").Trim("bubt.")
	paths := bubtsetts.Strings("diskpaths")
	mmap := bubtsetts.Bool("mmap")

	for _, path := range paths {
		fis, err := ioutil.ReadDir(path)
		if err != nil {
			log.Errorf("%v %v", bogn.logprefix, err)
			return
		}
		for _, fi := range fis {
			dirname := fi.Name()
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
		}
	}
	bogn.closelevels(disks[:]...)
	return
}

func (bogn *Bogn) destroydisksnaps(setts s.Settings) error {
	bubtsetts := setts.Section("bubt.").Trim("bubt.")
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
		index.Close()
	}
}

// destroy disk footprint in disk levels.
func (bogn *Bogn) destroylevels(indexes ...api.Index) {
	for _, index := range indexes {
		index.Close()
		index.Destroy()
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
	case *bubt.Snapshot:
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
