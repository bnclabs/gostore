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

// MinKeysize minimum key size.
const MinKeysize = int64(32)

// MaxKeysize maximum key size.
const MaxKeysize = int64(4096)

// MinValsize minimum value size.
const MinValsize = int64(0)

// MaxValsize maximum value size.
const MaxValsize = int64(10 * 1024 * 1024)
