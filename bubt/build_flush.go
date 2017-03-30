package bubt

import "fmt"

import "github.com/prataprc/golog"

var indexname, dataname = "index", "data"

type bubtflusher struct {
	f                *Bubt
	idxch, datach    chan []byte
	iquitch, dquitch chan struct{}
}

func (f *Bubt) startflusher() *bubtflusher {
	flusher := &bubtflusher{
		f:       f,
		idxch:   make(chan []byte, bufpoolSize),
		datach:  make(chan []byte, bufpoolSize),
		iquitch: make(chan struct{}),
		dquitch: make(chan struct{}),
	}

	go flusher.run(indexname, f.indexfd, flusher.idxch, flusher.iquitch)
	if f.hasdatafile {
		go flusher.run(dataname, f.datafd, flusher.datach, flusher.dquitch)
	}
	return flusher
}

func (flusher *bubtflusher) writeidx(data []byte) error {
	if len(data) > 0 {
		select {
		case flusher.idxch <- data:
		case <-flusher.iquitch:
			return fmt.Errorf("flusher.indexfile.closed")
		}
	}
	return nil
}

func (flusher *bubtflusher) writedata(data []byte) error {
	if len(data) > 0 {
		select {
		case flusher.datach <- data:
		case <-flusher.dquitch:
			return fmt.Errorf("flusher.datafile.closed")
		}
	}
	return nil
}

func (flusher *bubtflusher) close() {
	if flusher.f.hasdatafile {
		log.Infof("%v closing %q flusher ...\n", flusher.f.logprefix, dataname)
		close(flusher.datach)
		<-flusher.dquitch
	}

	log.Infof("%v closing %q flusher ...\n", flusher.f.logprefix, indexname)
	close(flusher.idxch)
	<-flusher.iquitch
}
