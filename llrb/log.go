package llrb

import "sync/atomic"

import "github.com/bnclabs/golog"

var logok = int64(0)

// LogComponents enable logging. By default logging is disabled,
// if applications want log information for llrb components
// call this function with "self" or "all" or "llrb" or "mvcc" as
// argument.
func LogComponents(components ...string) {
	for _, comp := range components {
		switch comp {
		case "llrb", "mvcc", "self", "all":
			atomic.StoreInt64(&logok, 1)
		}
	}
}

func debugf(format string, v ...interface{}) {
	if atomic.LoadInt64(&logok) > 0 {
		log.Debugf(format, v...)
	}
}

func errorf(format string, v ...interface{}) {
	if atomic.LoadInt64(&logok) > 0 {
		log.Errorf(format, v...)
	}
}

func fatalf(format string, v ...interface{}) {
	if atomic.LoadInt64(&logok) > 0 {
		log.Fatalf(format, v...)
	}
}

func infof(format string, v ...interface{}) {
	if atomic.LoadInt64(&logok) > 0 {
		log.Infof(format, v...)
	}
}

func tracef(format string, v ...interface{}) {
	if atomic.LoadInt64(&logok) > 0 {
		log.Tracef(format, v...)
	}
}

func verbosef(format string, v ...interface{}) {
	if atomic.LoadInt64(&logok) > 0 {
		log.Verbosef(format, v...)
	}
}

func warnf(format string, v ...interface{}) {
	if atomic.LoadInt64(&logok) > 0 {
		log.Warnf(format, v...)
	}
}
