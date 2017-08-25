package api

// Keyflags can be used with ParametrisedKey type.
type Keyflags uint16

const (
	keyBlack   Keyflags = 0x1
	keyDirty   Keyflags = 0x2
	keyDeleted Keyflags = 0x4
)

//---- keyBlack

// Setblack flag for llrb node.
func (f Keyflags) Setblack() Keyflags {
	return f | keyBlack
}

// Setred flag for llrb node.
func (f Keyflags) Setred() Keyflags {
	return f & (^(keyBlack)) // clear the bit
}

// Togglelink toggle red/black flag to black/red flag in llrb node.
func (f Keyflags) Togglelink() Keyflags {
	return f ^ keyBlack
}

// Isblack for llrb node.
func (f Keyflags) Isblack() bool {
	return (f & keyBlack) == keyBlack
}

// Isred for llrb node.
func (f Keyflags) Isred() bool {
	return !f.Isblack()
}

//---- keyDirty

// Setdirty set index node dirty.
func (f Keyflags) Setdirty() Keyflags {
	return f | keyDirty
}

// Cleardirty clear index node from dirty.
func (f Keyflags) Cleardirty() Keyflags {
	return f & (^(keyDirty))
}

// Isdirty check index node is dirty.
func (f Keyflags) Isdirty() bool {
	return (f & keyDirty) == keyDirty
}

//---- keyDeleted

// Setdeleted mark index node as deleted. After marking it as deleted
// there is no going back.
func (f Keyflags) Setdeleted() Keyflags {
	return f | keyDeleted
}

// Cleardeleted clear index node from deleted.
func (f Keyflags) Cleardeleted() Keyflags {
	return f & (^(keyDeleted))
}

// Isdeleted check whether index node is marked deleted.
func (f Keyflags) Isdeleted() bool {
	return (f & keyDeleted) == keyDeleted
}
