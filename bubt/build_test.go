package bubt

import "os"
import "testing"
import "path/filepath"
import "encoding/binary"

import "github.com/prataprc/gostore/llrb"
import s "github.com/prataprc/gosettings"

func TestDestroy(t *testing.T) {
	//mi := makeLLRB(1000)
	paths := makepaths()
	t.Log(paths)

	name := "testbuild"
	bubt := NewBubt(name, paths, 4096 /*msize*/, 4096 /*zsize*/)
	bubt.Close()

	snap, err := OpenSnapshot(name, paths, false /*mmap*/)
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Destroy()

	//iter := mi.Scan()
	//bubt.Build(iter, []byte("hello world"))
}

func makeLLRB(n int) *llrb.LLRB {
	var scratch [8]byte

	setts := s.Settings{
		"keycapacity": 10 * 1024 * 1024, "valcapacity": 10 * 1024 * 1024,
	}
	mi := llrb.NewLLRB("buildllrb", setts)
	k, v := []byte("key000000000000"), []byte("val00000000000000")
	for i := 0; i < n; i++ {
		binary.BigEndian.PutUint64(scratch[:], uint64(i+1))
		key, val := append(k[:3], scratch[:]...), append(v[:3], scratch[:]...)
		mi.Set(key, val, nil)
	}
	return mi
}

func makepaths() []string {
	path, paths := os.TempDir(), []string{}
	for _, base := range []string{"1", "2", "3"} {
		paths = append(paths, filepath.Join(path, base))
	}
	return paths
}
