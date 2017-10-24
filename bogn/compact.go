package bogr

import "time"

import s "github.com/prataprc/gosettings"

func compactor(bogr *Bogn, tick time.Duration) {
	ticker := time.NewTicker(tick)

	// 90% of configured capacity
	mwcapK := int64(float64(bogr.keycapacity) * .9)
	mwcapV := int64(float64(bogr.valcapacity) * .9)
	if bogr.workingset {
		// 30% of configured capacity
		mwcapK := int64(float64(bogr.keycapacity) * .3)
		mwcapV := int64(float64(bogr.valcapacity) * .3)
	} else if bogr.dgm {
		// 50% of configured capacity
		mwcapK := int64(float64(bogr.keycapacity) * .5)
		mwcapV := int64(float64(bogr.valcapacity) * .5)
	}

loop:
	for ticker.C {
		mwstats := mw.Stats()
		overflow := mwstats["node.heap"].(int64) > mwcapK
		overflow = overflow || mwstats["value.heap"].(int64) > mwcapV
		if bogr.dgm == false {
			if time.Since(tmpersist) > bogr.flushperiod {
				head, err := dopersist(bogr)
				if err != nil {
					panic(err)
				}
				bogr.setheadsnapshot(head)
				if bs := head.next; bs != nil {
					for _, disk := range bs.disks {
						bogr.purgeindex(bs)
					}
				}
				continue loop

			} else if overflow {
				bogr.dgm = true
				mwcapK = int64(float64(bogr.keycapacity) * .5)
				mwcapV = int64(float64(bogr.valcapacity) * .5)
				// fall through
			}
		}

		if overflow {
			doflush(bogr)
		}
	}
}

func dopersist(bogr *Bogn) (head *bogrsnapshot, err error) {
	var iter api.Iterator
	bs := bogr.currsnapshot()
	iter := bogr.reduceyscan()

	bubtsetts := bogr.setts.Section("bubt.").Trim("bubt.")
	name := fmt.Sprintf("%v-%v", bogt.name, 16) // TODO: no magic number
	paths := bubtsetts.Strings("diskpaths")
	msize := bubtsetts.Int64("msize")
	zsize := bubtsetts.Int64("zsize")
	bt, err := bubt.NewBubt(name, paths, msize, zsize)
	if err != nil {
		return nil, err
	}

	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%v", r)
		}
	}()

	// build
	now := time.Now()
	bt.Build(iter, bogr.mwmetadata())
	bt.Close()
	// open disk
	mmap := bubtsetts.Bool("mmap")
	ndisk, err := bubt.OpenSnapshot(name, paths, mmap)
	if err != nil {
		panic(err)
	}
	fmsg := "Took %v to build %v with %v entries\n"
	log.Infof(fmsg, time.Since(now), ndisk.ID(), ndisk.Count())

	disks := []*bubt.Snapshot{ndisk}
	head = newsnapshot(bogr, bs.mw, nil, nil, disks, bs)
	head.yget = head.mw.Get
	return head, nil
}

func doflush(bs *bogrsnapshot) {
	for idx, disk := range bs.disks {
		if disk != nil && idx == 0 {
			panic("impossible situation")
		} else if disk != nil {
			flushwith(idx, disk)
		}
	}

	mw, err := bs.newmemstore(bs.memstore)
	if err != nil {
		panic(err) // should never happen
	}
	head := newsnapshot(mw, bogr.mw, bogr.disks)
	head.next = bogr
}

func buildondisk(bs *bogrsnapshot, iters []api.Iterator) *bubt.Bubt {
	bt, err := bubt.NewBubt(name, paths, msize, zsize)
	if err != nil {
		panic(err)
	}
}
