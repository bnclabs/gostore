package bubt

import "os"
import "fmt"

import "github.com/prataprc/golog"

type bubtflusher struct {
	idx       int
	blocksize int
	file      string
	fd        *os.File
	ch        chan []byte
	quitch    chan struct{}
}

func startflusher(idx, blocksize int, file string) *bubtflusher {
	flusher := &bubtflusher{
		idx:       idx,
		blocksize: blocksize,
		file:      file,
		ch:        make(chan []byte, 100), // TODO: no magic number
		quitch:    make(chan struct{}),
	}
	if err := os.MkdirAll(filepath.Dir(file), 0770); err != nil {
		panic(fmt.Errorf("MkdirAll(%q)\n", mpath))
	} else {
		flusher.fd = createfile(flusher.file)
	}
	go flusher.run()
	return flusher
}

func (flusher *bubtflusher) writedata(data []byte) error {
	if len(data) != flusher.blocksize {
		err := fmt.Errorf("impossible situation, flushing %v bytes", len(data))
		panic(err)
	}
	select {
	case flusher.ch <- data:
	case <-flusher.quitch:
		return fmt.Errorf("flusher-%v.closed", flusher.idx)
	}
	return nil
}

func (flusher *bubtflusher) close() {
	log.Infof("%v closing %q flusher ...\n", flusher.f.logprefix, indexname)
	close(flusher.ch)
	<-flusher.quitch
}

func (flusher *bubtflusher) run() {
	logprefix := flusher.f.logprefix
	log.Infof("%v starting %q flusher for %v ...\n", logprefix, name, fd.Name())

	defer func() {
		log.Infof("%v exiting %q flusher for %v\n", logprefix, name, fd.Name())
		fd.Sync()
		close(flusher.quitch)
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
		if rc := write(block); rc == false {
			return
		}
	}

	// flush marker block
	markerblock := make([]byte, MarkerBlocksize)
	for i := 0; i < len(markerblock); i++ {
		markerblock[i] = MarkerByte
	}
	fmsg := "%v %q flusher writing marker block for %v\n"
	log.Infof(fmsg, logprefix, name, fd.Name())
	write(markerblock)
}
