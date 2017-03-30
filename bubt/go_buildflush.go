package bubt

import "os"

import "github.com/prataprc/golog"

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
