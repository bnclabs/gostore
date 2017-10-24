package bogn

import "time"

import s "github.com/prataprc/gosettings"

func compactor(bogn *Bogn, tick time.Duration) {
	log.Infof("%v starting compactor", bogn.logprefix)
	defer func() {
		close(bogn.purgech)
		if r := recover(); r != nil {
			log.Errorf("%v compactor crashed %v", bogn.logprefix, r)
			log.Errorf("\n%s", lib.GetStacktrace(2, debug.Stack()))
		} else {
			log.Infof("%v stopped compactor", bogn.logprefix)
		}
	}()

	ticker := time.NewTicker(tick)

	// 90% of configured capacity
	mwcap := int64(float64(bogn.keycapacity+bogn.valcapacity) * .9)
	if bogn.workingset {
		// 30% of configured capacity
		mwcap = int64(float64(bogn.keycapacity+bogn.valcapacity) * .3)
	} else if bogn.dgm {
		// 50% of configured capacity
		mwcap = int64(float64(bogn.valcapacity+bogn.valcapacity) * .5)
	}

	tmpersist := time.Now()

loop:
	for ticker.C {
		if bogn.isclosed() {
			break loop
		}

		mwstats := mw.Stats()
		heap := mwstats["node.heap"].(int64) + mwstats["value.heap"].(int64)
		overflow = heap > mwcap

		if bogn.dgm == false {
			if time.Since(tmpersist) > bogn.flushperiod {
				tmpersist = time.Now() // reload
				if err := dopersist(bogn); err != nil {
					panic(err)
				}
				continue loop

			} else if overflow {
				bogn.dgm = true
				mwcap = int64(float64(bogn.keycapacity+bogn.valcapacity) * .5)
				// fall through
			}
		}

		if overflow == false {
			bs := bogn.currsnapshot()
			var disk0 api.Index
			var level0 int
			for level1, disk1 := range bs.disks {
				if disk0 == nil {
					level0, disk0 = level1, disk1
					continue
				} else if disk1 == nil {
					continue
				}
				footprint0 := float64(disk0.(*bubt.Snapshot).Footprint())
				footprint1 := float64(disk1.(*bubt.Snapshot).Footprint())
				if (footprint0 / footprint1) > bogn.ratio {
					err := docompact(bogn, level0, level1, disk0, disk1)
					if err != nil {
						panic(err)
					}
					continue loop
				}
			}

		} else {
			if err := doflush(bogn); err != nil {
				panic(err)
			}
		}
	}
}

func dopersist(bogn *Bogn) (err error) {
	var iter api.Iterator
	bs := bogn.currsnapshot()
	iter := bogn.iterator()

	// create disk index
	level := len(bs.disks) - 1
	bt, err := newdiskstore(level)
	if err != nil {
		return err
	}

	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%v", r)
		}
	}()

	// build
	now := time.Now()
	bt.Build(iter, bogn.mwmetadata())
	bt.Close()
	// open disk
	bubtsetts := bogn.setts.Section("bubt.").Trim("bubt.")
	mmap := bubtsetts.Bool("mmap")
	paths := bubtsetts.Strings("diskpaths")
	ndisk, err := bubt.OpenSnapshot(bt.ID(), paths, mmap)
	if err != nil {
		panic(err)
	}
	fmsg := "Took %v to build %v with %v entries\n"
	log.Infof(fmsg, time.Since(now), ndisk.ID(), ndisk.Count())

	var disks [16]api.Index
	disks[16] = ndisk

	func() {
		bogn.rw.Lock()
		head = newsnapshot(bogn, bs.mw, nil, nil, disks)
		head.yget = head.mw.Get
		bogn.setheadsnapshot(head)
		bogn.rw.Unlock()
	}()

	for _, disk := range bs.disklevels() {
		bogn.purgeindex(disk)
	}
	return nil
}

func doflush(bogn *Bogn) (err error) {
	bs := bogn.currsnapshot()
	level, disk := bs.latestlevel()
	if level < 0 {
		level = len(bs.disks) - 1
	} else if disk != nil {
		footprint := float64(disk.(*bubt.Snapshot).Footprint())
		if (float64(bs.memheap()) / footprint) > bogn.ratio {
			level, disk = level-1, nil
		}
	}
	if level < 0 {
		panic("impossible situation")
	}

	iter := bs.flushiterator(disk)
	bt, err := newdiskstore(level)
	if err != nil {
		return err
	}

	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%v", r)
		}
	}()

	// build
	now := time.Now()
	bt.Build(iter, bogn.mwmetadata())
	bt.Close()
	// open disk
	bubtsetts := bogn.setts.Section("bubt.").Trim("bubt.")
	mmap := bubtsetts.Bool("mmap")
	paths := bubtsetts.Strings("diskpaths")
	ndisk, err := bubt.OpenSnapshot(bt.ID(), paths, mmap)
	if err != nil {
		panic(err)
	}
	fmsg := "Took %v to build %v with %v entries\n"
	log.Infof(fmsg, time.Since(now), ndisk.ID(), ndisk.Count())

	var disks [16]api.Index
	var mw, mc api.Index

	copy(disks, bs.disks)
	disks[level] = ndisk
	if bogn.workingset {
		mc, err = bs.newmemstore(bs.memstore, "mc", nil)
		if err != nil {
			panic(err) // should never happen
		}
	}

	func() {
		bogn.rw.Lock()
		mw, err = bs.newmemstore(bs.memstore, "mw", bs.mw)
		if err != nil {
			panic(err) // should never happen
		}
		head = newsnapshot(bogn, mw, bs.mw, mc, disks)
		bogn.setheadsnapshot(head)
		bogn.rw.Unlock()
	}()

	bogn.purgeindex(bs.mr)
	bogn.purgeindex(bs.mc)
	bogn.purgeindex(disk)
}

func docompact(bogn *Bogn, l0, l1 int, d0, d1 api.Index) (err error) {
	level, iter := l0, bs.compactiterator(disk0, disk1)
	bt, err := newdiskstore(level)
	if err != nil {
		return err
	}

	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%v", r)
		}
	}()

	// build
	now := time.Now()
	bt.Build(iter, bogn.diskmetadata(d0, d1))
	bt.Close()
	// open disk
	bubtsetts := bogn.setts.Section("bubt.").Trim("bubt.")
	mmap := bubtsetts.Bool("mmap")
	paths := bubtsetts.Strings("diskpaths")
	ndisk, err := bubt.OpenSnapshot(bt.ID(), paths, mmap)
	if err != nil {
		panic(err)
	}
	fmsg := "Took %v to build %v with %v entries\n"
	log.Infof(fmsg, time.Since(now), ndisk.ID(), ndisk.Count())

	var disks [16]api.Index
	copy(disks, bs.disks)
	disks[level] = ndisk

	func() {
		bogn.rw.Lock()
		head = newsnapshot(bogn, bs.mw, bs.mr, bs.mc, disks)
		bogn.setheadsnapshot(head)
		bogn.rw.Unlock()
	}()
	bogn.purgeindex(d0)
	bogn.purgeindex(d1)
}
