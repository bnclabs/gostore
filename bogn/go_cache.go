package bogn

import "runtime/debug"

import "github.com/prataprc/gostore/api"
import "github.com/prataprc/gostore/lib"
import "github.com/prataprc/golog"

type setcache struct {
	key     []byte
	value   []byte
	seqno   []byte
	deleted bool
}

func cacher(bogn *Bogn, mc api.Index, setch, cachech chan *setcache) {
	log.Infof("%v starting cacher %s", bogn.logprefix, mc.ID())
	defer func() {
		mc.Destroy()
		if r := recover(); r != nil {
			log.Errorf("%v cacher crashed %v", bogn.logprefix, r)
			log.Errorf("\n%s", lib.GetStacktrace(2, debug.Stack()))
		} else {
			log.Infof("%v stopped cacher %s", bogn.logprefix, mc.ID())
		}
	}()

	for cmd := range setch {
		mc.Setseqno(cmd.seqno - 1)
		if cmd.deleted { // delete in lsm mode.
			if _, cas := mc.Delete(key, nil, true); cas != cmd.seqno {
				panic("impossible situation")
			}
		} else if _, cas := mc.Set(key, value, nil); cas != cmd.seqno {
			panic("impossible situation")
		}
		cachech <- cmd
	}
}
