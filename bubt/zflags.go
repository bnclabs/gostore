package bubt

type zentryFlags uint16

const (
	zentryValfile zentryFlags = 0x1
	zentryDeleted zentryFlags = 0x2
)

//---- set flags

func (flags zentryFlags) setvalfile() zentryFlags {
	return flags | zentryFlags(zentryValfile)
}

func (flags zentryFlags) setdeleted() zentryFlags {
	return flags | zentryFlags(zentryDeleted)
}

func (flags zentryFlags) clearvalfile() zentryFlags {
	return flags & (^(zentryFlags(zentryValfile)))
}

func (flags zentryFlags) cleardeleted() zentryFlags {
	return flags & (^(zentryFlags(zentryDeleted)))
}

//---- get flags

func (flags zentryFlags) isvalfile() bool {
	return (flags & zentryValfile) != 0
}

func (flags zentryFlags) isdeleted() bool {
	return (flags & zentryDeleted) != 0
}
