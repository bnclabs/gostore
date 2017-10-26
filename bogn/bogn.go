package bogn

import "fmt"
import "sync"
import "unsafe"
import "time"
import "strings"
import "strconv"
import "io/ioutil"
import "sync/atomic"
import "encoding/json"

import "github.com/prataprc/gostore/api"
import "github.com/prataprc/gostore/lib"
import "github.com/prataprc/gostore/llrb"
import "github.com/prataprc/gostore/bubt"
import "github.com/prataprc/golog"
import s "github.com/prataprc/gosettings"

// Bogn instance to index key,value pairs.
type Bogn struct {
	nroutines int64 // atomic access, 8-byte aligned
	dgmstate  int64

	name     string
	snapshot unsafe.Pointer // *snapshot
	finch    chan struct{}
	snaprw   sync.RWMutex

	memcapacity int64
	memstore    string
	dgm         bool
	workingset  bool
	ratio       float64
	flushperiod time.Duration
	setts       s.Settings
	logprefix   string
}

// New create a new bogn instance.
func New(name string, setts s.Settings) (*Bogn, error) {
	bogn := (&Bogn{name: name}).readsettings(setts)
	bogn.finch = make(chan struct{})
	bogn.logprefix = fmt.Sprintf("BOGN [%v]", name)

	disks, err := bogn.opendisklevels(setts)
	if err != nil {
		bogn.Close()
		return nil, err
	}
	head, err := opensnapshot(bogn, disks)
	if err != nil {
		bogn.Close()
		return nil, err
	}
	bogn.setheadsnapshot(head)

	go purger(bogn)
	go compactor(bogn, bogn.flushperiod)

	log.Infof("%v started", bogn.logprefix)
	return bogn, nil
}

func (bogn *Bogn) readsettings(setts s.Settings) *Bogn {
	bogn.memstore = setts.String("memstore")
	switch bogn.memstore {
	case "llrb", "mvcc":
		llrbsetts := bogn.setts.Section("llrb.").Trim("llrb.")
		bogn.memcapacity = llrbsetts.Int64("keycapacity")
		bogn.memcapacity += llrbsetts.Int64("valcapacity")
	}
	bogn.dgm = setts.Bool("dgm")
	bogn.workingset = setts.Bool("workingset")
	bogn.ratio = setts.Float64("ratio")
	bogn.flushperiod = time.Duration(setts.Int64("flushperiod")) * time.Second
	bogn.setts = setts
	atomic.StoreInt64(&bogn.dgmstate, 0)
	if bogn.dgm {
		atomic.StoreInt64(&bogn.dgmstate, 1)
	}
	return bogn
}

func (bogn *Bogn) currsnapshot() *snapshot {
	return (*snapshot)(atomic.LoadPointer(&bogn.snapshot))
}

func (bogn *Bogn) setheadsnapshot(snapshot *snapshot) {
	atomic.StorePointer(&bogn.snapshot, unsafe.Pointer(snapshot))
}

func (bogn *Bogn) newmemstore(level string, old api.Index) (api.Index, error) {
	name := fmt.Sprintf("%v-%v", bogn.name, level)

	switch bogn.memstore {
	case "llrb":
		llrbsetts := bogn.setts.Section("llrb.").Trim("llrb.")
		index := llrb.NewLLRB(name, llrbsetts)
		if old != nil {
			index.Setseqno(old.(*llrb.LLRB).Getseqno())
		} else {
			index.Setseqno(0)
		}
		return index, nil

	case "mvcc":
		llrbsetts := bogn.setts.Section("llrb.").Trim("llrb.")
		index := llrb.NewMVCC(name, llrbsetts)
		if old != nil {
			index.Setseqno(old.(*llrb.LLRB).Getseqno())
		} else {
			index.Setseqno(0)
		}
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

func (bogn *Bogn) opendisklevels(setts s.Settings) ([16]api.Index, error) {
	var disks [16]api.Index
	var err error

	bubtsetts := bogn.setts.Section("bubt.").Trim("bubt.")
	paths := bubtsetts.Strings("diskpaths")
	mmap := bubtsetts.Bool("mmap")

	versions, uuids := map[int]int{}, map[int]string{}
	for _, path := range paths {
		fis, err := ioutil.ReadDir(path)
		if err != nil {
			return disks, err
		}
		for _, fi := range fis {
			level, version, uuid := bogn.path2level(fi.Name())
			if level < 0 {
				continue
			}
			if oldver, ok := versions[level]; !ok || oldver < version {
				versions[level] = version
				uuids[level*1000+version] = uuid
			}
		}
	}

	for level, version := range versions {
		uuid := uuids[level*1000+version]
		name := bogn.levelname(level, version, uuid)
		disks[level], err = bubt.OpenSnapshot(name, paths, mmap)
		if err != nil {
			return disks, err
		}
	}

	return disks, nil
}

// TODO: purge old snapshots and older versions.

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
			if time.Since(time.Unix(int64(x), 0)) > bogn.flushperiod {
				return true
			}
		}
	}
	return false
}

func (bogn *Bogn) pickflushdisk() (level, version int, disk api.Index) {
	snap := bogn.currsnapshot()

	if level, disk := snap.latestlevel(); level < 0 {
		return len(snap.disks) - 1, 1, nil

	} else if disk != nil {
		_, version, _ = bogn.path2level(disk.ID())
		footprint := float64(disk.(*bubt.Snapshot).Footprint())
		if (float64(snap.memheap()) / footprint) > bogn.ratio {
			return level - 1, 1, nil
		}
		return level, version + 1, disk
	}
	panic("unreachable code")
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

func (bogn *Bogn) ID() string {
	return bogn.name
}

func (bogn *Bogn) BeginTxn(id uint64) api.Transactor {
	return nil
}

func (bogn *Bogn) View(id uint64) api.Transactor {
	return nil
}

func (bogn *Bogn) Clone(id uint64) *Bogn {
	return nil
}

func (bogn *Bogn) Stats() map[string]interface{} {
	return nil
}

func (bogn *Bogn) Log() {
	return
}

func (bogn *Bogn) Close() {
	close(bogn.finch)
	for atomic.LoadInt64(&bogn.nroutines) > 0 { // wait till all routines exit
		time.Sleep(100 * time.Millisecond)
	}
	bogn.currsnapshot().close()
}

func (bogn *Bogn) Destroy() {
	bogn.Close()
	// TODO: cleanup disk footprint.
}

//---- Exported read methods

func (bogn *Bogn) Get(
	key, value []byte) (v []byte, cas uint64, deleted, ok bool) {

	return bogn.currsnapshot().yget(key, value)
}

func (bogn *Bogn) Scan() api.Iterator {
	return bogn.currsnapshot().iterator()
}

//---- Exported write methods

func (bogn *Bogn) Set(key, value, oldvalue []byte) (ov []byte, cas uint64) {
	bogn.snaprw.RLock()
	ov, cas = bogn.currsnapshot().set(key, value, oldvalue)
	bogn.snaprw.RUnlock()
	return ov, cas
}

func (bogn *Bogn) SetCAS(
	key, value, oldvalue []byte, cas uint64) ([]byte, uint64, error) {

	bogn.snaprw.RLock()
	ov, cas, err := bogn.currsnapshot().setCAS(key, value, oldvalue, cas)
	bogn.snaprw.RUnlock()
	return ov, cas, err
}

func (bogn *Bogn) Delete(key, oldvalue []byte) ([]byte, uint64) {
	bogn.snaprw.RLock()
	lsm := false
	if atomic.LoadInt64(&bogn.dgmstate) == 1 { // auto-enable lsm in dgm
		lsm = true
	}
	ov, cas := bogn.currsnapshot().delete(key, oldvalue, lsm)
	bogn.snaprw.RUnlock()
	return ov, cas
}

//---- local methods