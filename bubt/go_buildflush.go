package bubt

import "os"
import "fmt"
import "path/filepath"

import "github.com/prataprc/golog"

type bubtflusher struct {
	idx       int64
	fpos      int64
	blocksize int64
	file      string
	fd        *os.File
	ch        chan []byte
	quitch    chan struct{}
	blocks    chan []byte
}

func startflusher(idx, blocksize int, file string) (*bubtflusher, error) {
	flusher := &bubtflusher{
		idx:       int64(idx),
		fpos:      0,
		blocksize: int64(blocksize),
		file:      file,
		ch:        make(chan []byte, 100),
		quitch:    make(chan struct{}),
		blocks:    make(chan []byte, 100),
	}
	path := filepath.Dir(file)
	if err := os.MkdirAll(path, 0770); err != nil {
		return nil, err
	} else {
		flusher.fd = createfile(flusher.file)
	}
	go flusher.run()
	return flusher, nil
}

func (flusher *bubtflusher) writedata(data []byte) error {
	if int64(len(data)) != flusher.blocksize {
		panic(fmt.Errorf("impossible situation, flushing %v", len(data)))
	}
	block := flusher.getblock()
	copy(block, data)
	select {
	case flusher.ch <- block:
	case <-flusher.quitch:
		return fmt.Errorf("flusher-%v.closed", flusher.idx)
	}
	flusher.fpos += flusher.blocksize
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
			log.Fatalf("flusher(%q): %v", flusher.file, err)
		} else if n != len(block) {
			fmsg := "flusher(%q) partial write %v<%v"
			log.Errorf(fmsg, flusher.file, n, len(block))
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

	// flush marker block
	markerblock := make([]byte, MarkerBlocksize)
	for i := 0; i < len(markerblock); i++ {
		markerblock[i] = MarkerByte
	}
	write(markerblock)
}

func (flusher *bubtflusher) getblock() (block []byte) {
	select {
	case block = <-flusher.blocks:
	default:
		block = make([]byte, flusher.blocksize)
	}
	return
}

func (flusher *bubtflusher) putblock(block []byte) {
	select {
	case flusher.blocks <- block:
	default: // Leave it to GC
	}
}
