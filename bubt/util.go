package bubt

import "path/filepath"

func makespace(block []byte, off, ln int) []byte {
	for i, j := off+ln-1, ln-1; j >= 0; i, j = i-1, j-1 {
		block[i] = block[j]
	}
	for i := off + ln; i < len(block); i++ {
		block[i] = 0
	}
	return block
}

func mkfilenames(path string) (string, string) {
	index, data := filepath.Join(path, "index"), filepath.Join(path, "data")
	return index, data
}
