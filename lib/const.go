package lib

import "errors"

// ErrorUuidInvalidSize while generating uuid byte-string, the size of the
// byte-string shall be > 8 and shall be even numbered.
var ErrorUuidInvalidSize = errors.New("uuid.invalidsize")
