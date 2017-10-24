package bogn

import "runtime/debug"

import "github.com/prataprc/golog"
import "github.com/prataprc/gostore/api"
import "github.com/prataprc/gostore/lib"

func (bogn *Bogn) purgeindex(index api.Index) {
	bogn.purgech <- index
}

func purger(bogn *Bogn, purgech chan api.Index) {
	log.Infof("%v starting purger", bogn.logprefix)
	defer func() {
		if r := recover(); r != nil {
			log.Errorf("%v purger crashed %v", bogn.logprefix, r)
			log.Errorf("\n%s", lib.GetStacktrace(2, debug.Stack()))
		} else {
			log.Infof("%v stopped purger", bogn.logprefix)
		}
	}()

	for item := range purgech {
		if item == nil {
			continue
		}
		log.Infof("%v purging %q ...", bogn.logprefix, item.ID())
		item.Destroy()
	}
}
