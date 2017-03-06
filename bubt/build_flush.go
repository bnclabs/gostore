package bubt

import "os"
import "fmt"

import "github.com/prataprc/storage.go/log"

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

func (flusher *bubtflusher) run(
	name string, fd *os.File, ch chan []byte, quitch chan struct{}) {

	logprefix := flusher.f.logprefix
	log.Infof("%v starting %q flusher for %v ...\n", logprefix, name, fd.Name())

	defer func() {
		log.Infof("%v exiting %q flusher for %v\n", logprefix, name, fd.Name())
		fd.Sync()
		close(quitch)
	}()

	write := func(data []byte) bool {
		if n, err := fd.Write(data); err != nil {
			fmsg := "%v %q write %v: %v\n"
			log.Errorf(fmsg, logprefix, name, fd.Name(), err)
			return false
		} else if n != len(data) {
			fmsg := "%v partial %q write %v: %v<%v\n"
			log.Errorf(fmsg, logprefix, name, fd.Name(), n, len(data))
			return false
		}
		return true
	}

	// read byte blocks.
	for block := range ch {
		fmsg := "%v %q flusher writing block of len %v\n"
		log.Debugf(fmsg, logprefix, name, len(block))
		rc := write(block)
		if rc == false {
			return
		}
	}

	// flush marker block
	markerblock := make([]byte, MarkerBlocksize)
	for i := 0; i < len(markerblock); i++ {
		markerblock[i] = markerByte
	}
	fmsg := "%v %q flusher writing marker block for %v\n"
	log.Infof(fmsg, logprefix, name, fd.Name())
	write(markerblock)
}
