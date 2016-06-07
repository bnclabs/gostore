package bubt

import "os"
import "fmt"

import "github.com/prataprc/storage.go/log"

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

	go flusher.run(f.indexfd, flusher.idxch, flusher.iquitch)
	if f.hasdatafile() {
		go flusher.run(f.datafd, flusher.datach, flusher.dquitch)
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
	close(flusher.datach)
	<-flusher.dquitch
	// close and wait for index file to be sealed.
	close(flusher.idxch)
	<-flusher.iquitch
}

func (flusher *bubtflusher) run(fd *os.File, ch chan []byte, quitch chan struct{}) {
	logprefix := flusher.f.logprefix
	log.Infof("%v starting flusher for %v ...", logprefix, fd.Name())

	defer close(quitch)
	defer func() {
		log.Infof("%v exiting flusher for %v\n", logprefix, fd.Name())
	}()

	write := func(data []byte) bool {
		if n, err := fd.Write(data); err != nil {
			log.Errorf("%v write %v: %v", logprefix, fd.Name(), err)
			return false
		} else if n != len(data) {
			fmsg := "%v partial write %v: %v<%v)"
			log.Errorf(fmsg, logprefix, fd.Name(), n, len(data))
			return false
		}
		return true
	}

	// read byte blocks.
	for block := range ch {
		if write(block) == false {
			return
		}
	}

	// flush marker block
	markerblock := make([]byte, flusher.f.zblocksize)
	for i := 0; i < len(markerblock); i++ {
		markerblock[i] = markerByte
	}
	if write(markerblock) {
		log.Infof("%v wrote marker block for %v\n", logprefix, fd.Name())
	}
}
