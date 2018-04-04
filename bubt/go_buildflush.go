package bubt

import "os"
import "fmt"
import "path/filepath"

var maxqueue = 128

type bubtflusher struct {
	idx    int64
	fpos   int64
	vlog   []byte
	file   string
	mode   string
	fd     *os.File
	ch     chan *blockdata
	quitch chan struct{}
	pool   *blockpool
}

func startflusher(
	idx int, vsize int64, oldfile, newfile, mode string) (*bubtflusher, error) {

	flusher := &bubtflusher{
		idx:    int64(idx),
		fpos:   0,
		file:   newfile,
		mode:   mode,
		ch:     make(chan *blockdata, 128),
		quitch: make(chan struct{}),
		pool:   newblockpool(128),
	}
	if vsize > 0 {
		flusher.vlog = make([]byte, 0, vsize)
		flusher.fpos = int64(flusher.idx << 56)
	}
	path := filepath.Dir(newfile)
	if err := os.MkdirAll(path, 0770); err != nil {
		errorf("os.MkdirAll(%q): %v", path, err)
		return nil, err
	} else if mode == "create" {
		flusher.fd = createfile(newfile)

	} else if mode == "appendlink" {
		size := filesize(oldfile)
		if err := os.Truncate(oldfile, size-MarkerBlocksize); err != nil {
			panic(err)
		}
		flusher.fpos += (size - MarkerBlocksize)
		flusher.fd = appendlinkfile(oldfile, newfile)
	} else {
		panic(fmt.Errorf("invalid mode %q", mode))
	}
	go flusher.run()
	return flusher, nil
}

func (flusher *bubtflusher) writedata(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	block := flusher.pool.getblock(len(data))
	copy(block.data, data)
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

	write := func(block *blockdata) (rc bool) {
		//fmt.Println("loop", flusher.idx, len(block.data))
		if n, err := flusher.fd.Write(block.data); err != nil {
			fatalf("flusher(%q): %v", flusher.file, err)
		} else if n != len(block.data) {
			fmsg := "flusher(%q) partial write %v<%v"
			fatalf(fmsg, flusher.file, n, len(block.data))
		} else {
			rc = true
		}
		return
	}

	// read byte blocks.
	for block := range flusher.ch {
		if rc := write(block); rc == false {
			return
		}
		flusher.pool.putblock(block)
	}

	if len(flusher.vlog) > 0 {
		fmsg := "unexpected partial value log %v"
		panic(fmt.Errorf(fmsg, len(flusher.vlog)))
	}

	// flush marker block
	markerblock := make([]byte, MarkerBlocksize)
	for i := 0; i < len(markerblock); i++ {
		markerblock[i] = MarkerByte
	}
	write(&blockdata{data: markerblock})
}
