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

// TODO: enable count aggregation across snapshots, with data-structures
// that support LSM it is difficult to maintain accurate count.

// Bogn instance to index key,value pairs.
type Bogn struct {
	// atomic access, 8-byte aligned
	nroutines int64
	dgmstate  int64

	name     string
	snapshot unsafe.Pointer // *snapshot
	finch    chan struct{}
	snaprw   sync.RWMutex

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

	go purger(bogn)
	go compactor(bogn)

	return bogn, nil
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
		if snap.istrypurge() {
			return snap
		}
		snap.release()
		runtime.Gosched()
	}
	panic("unreachable code")
}

func (bogn *Bogn) mwmetadata(seqno uint64) []byte {
	metadata := map[string]interface{}{
		"seqno":     seqno,
		"flushunix": fmt.Sprintf("%q", time.Now().Unix()),
	}
	data, err := json.Marshal(metadata)
	if err != nil {
		panic(err)
	}
	return data
}

func (bogn *Bogn) flushelapsed() bool {
	var md map[string]interface{}
	if snap := bogn.currsnapshot(); snap != nil {
		if _, disk := snap.latestlevel(); disk != nil {
			var metadata []byte
			switch index := disk.(type) {
			case *bubt.Snapshot:
				metadata = index.Metadata()
			default:
				panic("impossible situation")
			}
			if err := json.Unmarshal(metadata, &md); err != nil {
				panic(err)
			}
			x, _ := strconv.Atoi(md["flushunix"].(string))
			if time.Since(time.Unix(int64(x), 0)) > bogn.period {
				return true
			}
		}
	}
	return false
}

func (bogn *Bogn) pickflushdisk(disk0 api.Index) (api.Index, int, int) {
	snap := bogn.currsnapshot()

	level, disk := snap.latestlevel()
	if level < 0 && disk0 != nil {
		panic("impossible situation")

	} else if level < 0 {
		return nil, len(snap.disks) - 1, 1

	} else if disk != nil {
		level0, _, _ := bogn.path2level(disk0.ID())
		if level0 <= level {
			panic("impossible situation")
		}
		if disk.ID() == disk0.ID() {
			return nil, level - 1, 1
		}
	}
	_, version, _ := bogn.path2level(disk.ID())
	footprint := float64(disk.(*bubt.Snapshot).Footprint())
	if (float64(snap.memheap()) / footprint) > bogn.ratio {
		return disk, level - 1, 1
	}
	return disk, level, version + 1
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

func (bogn *Bogn) path2level(filename string) (level, ver int, uuid string) {
	var err error

	parts := strings.Split(filename, "-")
	if len(parts) == 4 && parts[0] == bogn.name {
		if level, err = strconv.Atoi(parts[1]); err != nil {
			return -1, -1, ""
		}
		if ver, err = strconv.Atoi(parts[2]); err != nil {
			return -1, -1, ""
		}
		uuid = parts[3]
	}
	return
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

// TODO: to be implemented.
func (bogn *Bogn) BeginTxn(id uint64) api.Transactor {
	return nil
}

// TODO: to be implemented.
func (bogn *Bogn) View(id uint64) api.Transactor {
	return nil
}

func (bogn *Bogn) Log() {
	snap := bogn.latestsnapshot()
	if snap.mw != nil {
		log.Infof("%v write-store count %v\n", bogn.indexcount(snap.mw))
	}
	if snap.mr != nil {
		log.Infof("%v read-store count %v\n", bogn.indexcount(snap.mr))
	}
	if snap.mc != nil {
		log.Infof("%v cache-store count %v\n", bogn.indexcount(snap.mc))
	}
	for _, disk := range snap.disklevels([]api.Index{}) {
		log.Infof("%v disk-store count %v\n", bogn.indexcount(disk))
	}
	snap.release()
}

// Compact away oldver version of disk snapshots.
func (bogn *Bogn) Compact() {
	bogn.compactdisksnaps(bogn.setts)
}

// Close this instance, no calls allowed after Close.
func (bogn *Bogn) Close() {
	close(bogn.finch)
	for atomic.LoadInt64(&bogn.nroutines) > 0 {
		time.Sleep(100 * time.Millisecond)
	}
	for purgesnapshot(bogn.currsnapshot()) {
		time.Sleep(100 * time.Millisecond)
	}
	bogn.setheadsnapshot(nil)
}

// Destroy the disk footprint of this instance, no calls allowed
// after Close.
func (bogn *Bogn) Destroy() {
	bogn.destroydisksnaps(bogn.setts)
	return
}

//---- Exported read methods

// Get value for key, if value argument points to valid buffer it will, be
// used to copy the entry's value. Also returns entry's cas, whether entry
// is marked as deleted by LSM. If ok is false, then key is not found.
func (bogn *Bogn) Get(
	key, value []byte) (v []byte, cas uint64, deleted, ok bool) {

	snap := bogn.latestsnapshot()
	v, cas, deleted, ok = snap.yget(key, value)
	snap.release()
	return
}

// Scan return a full table iterator.
func (bogn *Bogn) Scan() api.Iterator {
	var err error
	snap := bogn.latestsnapshot()
	iter := snap.iterator()
	return func(fin bool) (key, val []byte, seqno uint64, del bool, e error) {
		if err == io.EOF {
			return nil, nil, 0, false, err

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
	bogn.snaprw.RLock()
	ov, cas = bogn.currsnapshot().set(key, value, oldvalue)
	bogn.snaprw.RUnlock()
	return ov, cas
}

// SetCAS a key, value pair in the index, if CAS is ZERO then key should
// not be present in the index, otherwise existing CAS should match the
// supplied CAS. Value will be over-written. Make sure key is not nil.
// Return old value if oldvalue points to valid buffer.
func (bogn *Bogn) SetCAS(
	key, value, oldvalue []byte, cas uint64) ([]byte, uint64, error) {

	bogn.snaprw.RLock()
	ov, cas, err := bogn.currsnapshot().setCAS(key, value, oldvalue, cas)
	bogn.snaprw.RUnlock()
	return ov, cas, err
}

// Delete key from index. Key should not be nil, if key found return its
// value. If lsm is true, then don't delete the node instead mark the node
// as deleted. Again, if lsm is true but key is not found in index, a new
// entry will inserted.
func (bogn *Bogn) Delete(key, oldvalue []byte, lsm bool) ([]byte, uint64) {
	bogn.snaprw.RLock()
	if atomic.LoadInt64(&bogn.dgmstate) == 1 { // auto-enable lsm in dgm
		lsm = true
	}
	ov, cas := bogn.currsnapshot().delete(key, oldvalue, lsm)
	bogn.snaprw.RUnlock()
	return ov, cas
}

//---- local methods

func (bogn *Bogn) newmemstore(level string, seqno uint64) (api.Index, error) {
	name := fmt.Sprintf("%v-%v", bogn.name, level)

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
	return nil, fmt.Errorf("invalid memstore %q", bogn.memstore)
}

func (bogn *Bogn) builddiskstore(
	level, ver int,
	iter api.Iterator) (index api.Index, count uint64, err error) {

	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%v", r)
		}
	}()

	// book-keep largest seqno for this snapshot.
	var diskseqno uint64

	wrap := func(fin bool) ([]byte, []byte, uint64, bool, error) {
		key, val, seqno, deleted, e := iter(fin)
		if seqno > diskseqno {
			diskseqno = seqno
		}
		count++
		return key, val, seqno, deleted, e
	}

	uuid := make([]byte, 64)
	n := lib.Uuid(make([]byte, 8)).Format(uuid)
	name := bogn.levelname(level, ver, string(uuid[:n]))

	bubtsetts := bogn.setts.Section("bubt.").Trim("bubt.")
	paths := bubtsetts.Strings("diskpaths")
	msize := bubtsetts.Int64("msize")
	zsize := bubtsetts.Int64("zsize")
	bt, err := bubt.NewBubt(name, paths, msize, zsize)
	if err != nil {
		return nil, count, err
	}
	defer bt.Close()

	// build
	if err = bt.Build(wrap, nil); err != nil {
		return nil, count, err
	} else if err = bt.Writemetadata(bogn.mwmetadata(diskseqno)); err != nil {
		return nil, count, err
	}

	// open disk
	mmap := bubtsetts.Bool("mmap")
	ndisk, err := bubt.OpenSnapshot(name, paths, mmap)
	if err != nil {
		return nil, count, err
	}
	return ndisk, count, nil
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
			if level, _, _ := bogn.path2level(fi.Name()); level < 0 {
				continue // not a bogn directory
			}
			disks, err = bogn.buildlevels(fi.Name(), paths, mmap, disks)
			if err != nil {
				return disks, err
			}
		}
	}
	for _, disk := range disks {
		log.Infof("%v open-snapshot %v", bogn.logprefix, disk.ID())
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
			if level, _, _ := bogn.path2level(fi.Name()); level < 0 {
				continue // not a bogn directory
			}
			disks = bogn.compactlevels(fi.Name(), paths, mmap, disks)
		}
	}
	bogn.closelevels(disks[:]...)
	return
}

func (bogn *Bogn) destroydisksnaps(setts s.Settings) error {
	bubtsetts := bogn.setts.Section("bubt.").Trim("bubt.")
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

// open snapshots for latest version in each level.
func (bogn *Bogn) buildlevels(
	filename string, paths []string, mmap bool,
	disks [16]api.Index) ([16]api.Index, error) {

	level, version, uuid := bogn.path2level(filename)
	if level < 0 { // not a bogn snapshot
		return disks, nil
	}

	name := bogn.levelname(level, version, uuid)
	disk, err := bubt.OpenSnapshot(name, paths, mmap)
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
	return disks, nil
}

// destory older versions if multiple versions are detected for a disk level
func (bogn *Bogn) compactlevels(
	filename string, paths []string, mmap bool,
	disks [16]api.Index) [16]api.Index {

	level, version, uuid := bogn.path2level(filename)
	if level < 0 { // not a bogn snapshot
		return disks
	}

	name := bogn.levelname(level, version, uuid)
	disk, err := bubt.OpenSnapshot(name, paths, mmap)
	if err != nil {
		bubt.PurgeSnapshot(name, paths)
		return disks
	}

	if od := disks[level]; od == nil { // first version
		disks[level] = disk

	} else if _, over, _ := bogn.path2level(od.ID()); over < version {
		bogn.destroylevels(disks[level])
		disks[level] = disk

	} else {
		bogn.destroylevels(disk)
	}
	return disks
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

func (bogn *Bogn) indexcount(index api.Index) int64 {
	switch idx := index.(type) {
	case *llrb.LLRB:
		return idx.Count()
	case *llrb.MVCC:
		return idx.Count()
	case *bubt.Snapshot:
		return idx.Count()
	}
	panic("impossible situation")
}
