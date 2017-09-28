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
}

func startflusher(idx, blocksize int, file string) *bubtflusher {
	flusher := &bubtflusher{
		idx:       int64(idx),
		fpos:      0,
		blocksize: int64(blocksize),
		file:      file,
		ch:        make(chan []byte, 100), // TODO: no magic number
		quitch:    make(chan struct{}),
	}
	path := filepath.Dir(file)
	if err := os.MkdirAll(path, 0770); err != nil {
		panic(fmt.Errorf("MkdirAll(%q)\n", path))
	} else {
		flusher.fd = createfile(flusher.file)
	}
	go flusher.run()
	return flusher
}

func (flusher *bubtflusher) writedata(data []byte) error {
	if int64(len(data)) != flusher.blocksize {
		err := fmt.Errorf("impossible situation, flushing %v bytes", len(data))
		panic(err)
	}
	select {
	case flusher.ch <- data:
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
		close(flusher.quitch)
	}()

	write := func(data []byte) bool {
		if n, err := flusher.fd.Write(data); err != nil {
			log.Fatalf("flusher(%q): %v", flusher.file, err)
			return false
		} else if n != len(data) {
			fmsg := "flusher(%q) partial write %v<%v"
			log.Errorf(fmsg, flusher.file, n, len(data))
			return false
		}
		return true
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
