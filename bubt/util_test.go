package bubt

import "os"
import "io"
import "testing"

func TestFileaccess(t *testing.T) {
	filename := "testfile"
	defer func() {
		if r := recover(); r != nil {
			t.Error(r)
		}
		os.Remove(filename)
	}()

	if fd := createfile(filename); fd == nil {
		t.Errorf("unexpected nil")
	} else {
		block := make([]byte, 1024*2)
		for i := range block {
			block[i] = byte(i % 256)
		}
		if n, err := fd.Write(block); err != nil {
			t.Error(err)
		} else if n < len(block) {
			t.Errorf("wrote %v", n)
		}
	}

	dotest := func(r io.ReaderAt) {
		block := make([]byte, 1024)
		n, err := r.ReadAt(block, 0)
		if err != nil {
			t.Error(err)
		} else if n < len(block) {
			t.Errorf("wrote %v", n)
		} else if block[1023] != (1023 % 256) {
			t.Errorf("unexpected %v", block[1023])
		}

		n, err = r.ReadAt(block, 1024)
		if err != nil {
			t.Error(err)
		} else if n < len(block) {
			t.Errorf("wrote %v", n)
		} else if block[0] != (1024 % 256) {
			t.Errorf("unexpected %v", block[1023])
		} else if block[1023] != (2047 % 256) {
			t.Errorf("unexpected %v", block[2047])
		}

		if x := filesize(r); x != 2048 {
			t.Errorf("unexpected %v", x)
		}
	}

	dotest(openfile(filename, false))
	dotest(openfile(filename, true))
}
