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

func zerosin8(v byte) (c int) {
	for c := 0; v != 0; v >>= 1 { // count set bits
		c += int(v & 1)
	}
	return 8 - c
}

func zerosin32(v uint32) (c int) {
	v = v - ((v >> 1) & 0x55555555)
	v = (v & 0x33333333) + ((v >> 2) & 0x33333333)
	return int((((v + (v >> 4)) & 0x0F0F0F0F) * 0x01010101) >> 24)
}
