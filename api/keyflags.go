package api

// Keyflags can be used with ParametrisedKey type.
type Keyflags uint16

const (
	keyBlack   = 0x1
	keyDirty   = 0x2
	keyDeleted = 0x4
)

//---- keyBlack

// Setblack flag for llrb node.
func (f Keyflags) Setblack() Keyflags {
	return f | Keyflags(keyBlack)
}

// Setred flag for llrb node.
func (f Keyflags) Setred() Keyflags {
	return f & (^(Keyflags(keyBlack))) // clear the bit
}

// Togglelink toggle red/black flag to black/red flag in llrb node.
func (f Keyflags) Togglelink() Keyflags {
	return f ^ Keyflags(keyBlack)
}

// Isblack for llrb node.
func (f Keyflags) Isblack() bool {
	return (f & keyBlack) == Keyflags(keyBlack)
}

// Isred for llrb node.
func (f Keyflags) Isred() bool {
	return !f.Isblack()
}

//---- keyDirty

// Setdirty set index node dirty.
func (f Keyflags) Setdirty() Keyflags {
	return f | Keyflags(keyDirty)
}

// Cleardirty clear index node from dirty.
func (f Keyflags) Cleardirty() Keyflags {
	return f & (^(Keyflags(keyDirty)))
}

// Isdirty check index node is dirty.
func (f Keyflags) Isdirty() bool {
	return (f & Keyflags(keyDirty)) == Keyflags(keyDirty)
}

//---- keyDeleted

// Setdeleted mark index node as deleted. After marking it as deleted
// there is no going back.
func (f Keyflags) Setdeleted() Keyflags {
	return f | Keyflags(keyDeleted)
}

// Cleardeleted clear index node from deleted.
func (f Keyflags) Cleardeleted() Keyflags {
	return f & (^(Keyflags(keyDeleted)))
}

// Isdeleted check whether index node is marked deleted.
func (f Keyflags) Isdeleted() bool {
	return (f & Keyflags(keyDeleted)) == Keyflags(keyDeleted)
}
