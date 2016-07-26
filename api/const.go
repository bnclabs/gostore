// storage constants.

package api

import "errors"

var ErrorActiveSnapshots = errors.New("activesnapshots")

var ErrorActiveIterators = errors.New("activeiterators")

// MinKeymem minimum key size.
const MinKeymem = int64(32)

// MaxKeymem maximum key size.
const MaxKeymem = int64(4096)

// MinValmem minimum value size.
const MinValmem = int64(0)

// MaxValmem maximum value size.
const MaxValmem = int64(10 * 1024 * 1024)
