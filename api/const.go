package api

import "errors"

// ErrorActiveSnapshots operation cannot succeed because there are active
// snapshots on the storage instance.
var ErrorActiveSnapshots = errors.New("activeSnapshots")

// ErrorActiveIterators operation cannot succeed because there are active
// iterators on the storage instance.
var ErrorActiveIterators = errors.New("activeIterators")

// ErrorKeyMissing operation cannot succeed because specifed key is missing
// in the storage instance.
var ErrorKeyMissing = errors.New("keyMissing")

// ErrorInvalidCAS operation cannot succeed because CAS value does not
// match with the document.
var ErrorInvalidCAS = errors.New("invalidCAS")

// MinKeymem minimum key size.
const MinKeymem = int64(32)

// MaxKeymem maximum key size.
const MaxKeymem = int64(4096)

// MinValmem minimum value size.
const MinValmem = int64(0)

// MaxValmem maximum value size.
const MaxValmem = int64(10 * 1024 * 1024)
