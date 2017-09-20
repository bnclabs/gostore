package llrb

import "testing"
import "hash/crc32"

func TestCRC32(t *testing.T) {
	tbl := crc32.MakeTable(crc32.IEEE)
	crc1 := crc32.Checksum([]byte("plumless"), tbl)
	crc2 := crc32.Checksum([]byte("buckeroo"), tbl)
	if crc1 != crc2 {
		t.Errorf("%x %x", crc1, crc2)
	}
}

func BenchmarkCRC32(b *testing.B) {
	tbl := crc32.MakeTable(crc32.IEEE)
	text := []byte("plumlessssssssssssssssssssssssssssssssssssss")
	for i := 0; i < b.N; i++ {
		crc32.Checksum(text, tbl)
	}
}
