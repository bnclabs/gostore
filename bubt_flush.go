// +build ignore

package storage

func (f *Bubtstore) writeidx(data []byte) error {
	select {
	case f.idxch <- data:
	case <-f.iquitch:
		return fmt.Errorf("data flusher exited")
	}
	return nil
}

func (f *Bubtstore) writedata(data []byte) error {
	if len(data) > 0 {
		select {
		case f.datach <- data:
		case <-f.dquitch:
			return fmt.Errorf("data flusher exited")
		}
		return nil
	}
	return nil
}

func (f *Bubtstore) flusher(fd *os.File, ch chan []byte, quitch chan struct{}) {
	log.Infof("%v starting flusher for %v ...", f.logprefix, fd.Name())
	defer close(quitch)
	defer func() {
		log.Infof("%v exiting flusher for %v\n", f.logprefix, fd.Name())
	}()

	write := func(data []byte) bool {
		if n, err := fd.Write(data); err != nil {
			log.Errorf("%v write %v: %v", f.logprefix, fd.Name(), err)
			return false
		} else if n != len(data) {
			fmsg := "%v partial write %v: %v<%v)"
			log.Errorf(fmsg, f.logprefix, fd.Name(), n, len(data))
			return false
		}
		return true
	}

	// read byte blocks.
	for block := range ch {
		if write(block) == false {
			return
		}
		f.putbuffer(block)
	}

	// flush marker block
	markerblock := make([]byte, 4096)
	for i := 0; i < len(markerblock); i++ {
		markerblock[i] = 0xAB
	}
	if write(markerblock) {
		log.Infof("%v wrote marker block for %v\n", f.logprefix, fd.Name())
	}
}
