package bubt

import "os"
import "fmt"

import "golang.org/x/exp/mmap"

func createfile(name string) *os.File {
	os.Remove(name)
	fd, err := os.OpenFile(name, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		panic(fmt.Errorf("create append file: %v", err))
	}
	return fd
}

func openfile(filename string, mmap bool) (r io.ReaderAt) {
	if mmap {
		r, err := mmap.Open(filename)
		if err != nil {
			panic(fmt.Errorf("mmap.Open(%q): %v", mfile, err))
		}
		return r
	}
	r, err := os.OpenFile(mfile, os.O_RDONLY, 0666)
	if err != nil {
		panic(fmt.Errorf("OpenFile(%q): %v", mfile, err))
	}
	return r
}

func filesize(r io.ReaderAt) int64 {
	switch x := r.(type) {
	case *mmap.ReaderAt:
		return int64(x.Len())

	case *os.File:
		fi, err := x.Stat()
		if err == nil {
			return int64(fi.Size())
		}
		panic(fmt.Errorf(err))
	}
	panic("unreachable code")
}
