package bubt

import "os"
import "fmt"
import "path/filepath"

type bubtflusher struct {
	idx    int64
	fpos   int64
	vlog   []byte
	file   string
	fd     *os.File
	ch     chan []byte
	quitch chan struct{}
	blocks chan []byte
}

func startflusher(idx int, vsize int64, file string) (*bubtflusher, error) {
	flusher := &bubtflusher{
		idx:    int64(idx),
		fpos:   0,
		file:   file,
		ch:     make(chan []byte, 100),
		quitch: make(chan struct{}),
		blocks: make(chan []byte, 100),
	}
	if vsize > 0 {
		flusher.vlog = make([]byte, 0, vsize)
		flusher.fpos = int64(flusher.idx << 56)
	}
	path := filepath.Dir(file)
	if err := os.MkdirAll(path, 0770); err != nil {
		errorf("os.MkdirAll(%q): %v", path, err)
		return nil, err
	} else {
		flusher.fd = createfile(flusher.file)
	}
	go flusher.run()
	return flusher, nil
}

func (flusher *bubtflusher) writedata(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	block := flusher.getblock(len(data))
	copy(block, data)
	select {
	case flusher.ch <- block:
	case <-flusher.quitch:
		return fmt.Errorf("flusher-%v.closed", flusher.idx)
	}
	flusher.fpos += int64(len(data))
	return nil
}

func (flusher *bubtflusher) close() {
	close(flusher.ch)
	<-flusher.quitch
}

func (flusher *bubtflusher) run() {
	defer func() {
		flusher.fd.Sync()
		flusher.fd.Close()
		close(flusher.quitch)
	}()

	write := func(block []byte) (rc bool) {
		if n, err := flusher.fd.Write(block); err != nil {
			fatalf("flusher(%q): %v", flusher.file, err)
		} else if n != len(block) {
			fmsg := "flusher(%q) partial write %v<%v"
			fatalf(fmsg, flusher.file, n, len(block))
		} else {
			rc = true
		}
		flusher.putblock(block)
		return
	}

	// read byte blocks.
	for block := range flusher.ch {
		if rc := write(block); rc == false {
			return
		}
	}

	// if vlog contains some data, flush them first.
	if len(flusher.vlog) > 0 {
		write(flusher.vlog)
	}

	// flush marker block
	markerblock := make([]byte, MarkerBlocksize)
	for i := 0; i < len(markerblock); i++ {
		markerblock[i] = MarkerByte
	}
	write(markerblock)
}

func (flusher *bubtflusher) getblock(size int) (block []byte) {
	select {
	case block = <-flusher.blocks:
	default:
		block = make([]byte, size)
	}
	if cap(block) < size {
		block = make([]byte, size)
	}
	block = block[:size]
	return
}

func (flusher *bubtflusher) putblock(block []byte) {
	select {
	case flusher.blocks <- block:
	default: // Leave it to GC
	}
}
