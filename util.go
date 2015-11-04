package llrb

// Move this to ASM.
func findfirstset8(b byte) uint8 {
	for i := uint8(0); i < 8; i++ {
		if (b & (1 << i)) != 0 {
			return uint8(i)
		}
	}
	panic("no bit set in the byte")
}

func clearbit8(b byte, n uint8) byte {
	return b & (0xff ^ (1 << n))
}

func setbit8(b byte, n uint8) byte {
	return b | (1 << n)
}
