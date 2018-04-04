package bubt

import "os"
import "io"
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

func appendlinkfile(oldfile, newfile string) *os.File {
	if oldfile != "" {
		if err := os.Link(oldfile, newfile); err != nil {
			panic(err)
		}
	}
	fd, err := os.OpenFile(newfile, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		panic(fmt.Errorf("append file: %v", err))
	}
	return fd
}

func openfile(filename string, ismmap bool) (r io.ReaderAt) {
	if ismmap {
		r, err := mmap.Open(filename)
		if err != nil {
			panic(fmt.Errorf("mmap.Open(%q): %v", filename, err))
		}
		return r
	}
	r, err := os.OpenFile(filename, os.O_RDONLY, 0666)
	if err != nil {
		panic(fmt.Errorf("OpenFile(%q): %v", filename, err))
	}
	return r
}

func closereadat(rd io.ReaderAt) error {
	if rd != nil {
		switch r := rd.(type) {
		case *os.File:
			return r.Close()
		case *mmap.ReaderAt:
			return r.Close()
		}
	}
	return nil
}

func filesize(r interface{}) int64 {
	if r == nil {
		return 0
	}
	switch x := r.(type) {
	case *mmap.ReaderAt:
		return int64(x.Len())

	case *os.File:
		fi, err := x.Stat()
		if err != nil {
			panic(err)
		}
		return int64(fi.Size())

	case string:
		fi, err := os.Stat(x)
		if err != nil {
			panic(err)
		}
		return fi.Size()
	}
	panic("unreachable code")
}
