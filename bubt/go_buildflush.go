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
	close(flusher.ch)
	<-flusher.quitch
}

func (flusher *bubtflusher) run() {
	defer func() {
		fd.Sync()
		close(flusher.quitch)
	}()

	write := func(data []byte) bool {
		if n, err := fd.Write(data); err != nil {
			log.Fatalf("flusher(%q): %v", flusher.file, err)
			return false
		} else if n != len(data) {
			log.Errorf("flusher(%q) partial write %v<%v", file, n, len(data))
			return false
		}
		return true
	}

	// read byte blocks.
	for block := range ch {
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
