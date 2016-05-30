package lib

// Bit32 alias for uint32, provides bit twiddling methods on 32-bit number.
type Bit32 uint32

// Ones return number of set bits in uint32 word.
func (b Bit32) Ones() int8 { // move this to ASM.
	b = b - ((b >> 1) & 0x55555555)
	b = (b & 0x33333333) + ((b >> 2) & 0x33333333)
	return int8((((b + (b >> 4)) & 0x0F0F0F0F) * 0x01010101) >> 24)
}

// Zeros return number of clear bits in uint32 word.
func (b Bit32) Zeros() int8 {
	return 32 - b.Ones()
}
