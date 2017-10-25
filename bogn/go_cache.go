package bogn

import "sync/atomic"
import "runtime/debug"

import "github.com/prataprc/gostore/api"
import "github.com/prataprc/gostore/lib"
import "github.com/prataprc/gostore/llrb"
import "github.com/prataprc/golog"

// setcache commands to cacher routine.
type setcache struct {
	key     []byte
	value   []byte
	seqno   uint64
	deleted bool
}

func cacher(bogn *Bogn, mc api.Index, setch, cachech chan *setcache) {
	atomic.AddInt64(&bogn.nroutines, 1)
	log.Infof("%v starting cacher %s", bogn.logprefix, mc.ID())
	defer func() {
		mc.Destroy()
		if r := recover(); r != nil {
			log.Errorf("%v cacher crashed %v", bogn.logprefix, r)
			log.Errorf("\n%s", lib.GetStacktrace(2, debug.Stack()))
		} else {
			log.Infof("%v stopped cacher %s", bogn.logprefix, mc.ID())
		}
		atomic.AddInt64(&bogn.nroutines, -1)
	}()

	setseqno := func(seqno uint64) {
		switch index := mc.(type) {
		case *llrb.LLRB:
			index.Setseqno(seqno)
		case *llrb.MVCC:
			index.Setseqno(seqno)
		}
	}

	for cmd := range setch {
		setseqno(cmd.seqno - 1)
		if cmd.deleted { // delete in lsm mode.
			if _, cas := mc.Delete(cmd.key, nil, true); cas != cmd.seqno {
				panic("impossible situation")
			}
		} else if _, cas := mc.Set(cmd.key, cmd.value, nil); cas != cmd.seqno {
			panic("impossible situation")
		}
		cachech <- cmd
	}
}
