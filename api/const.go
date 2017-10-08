package api

import "errors"

// ErrorInvalidCAS operation cannot succeed because CAS value does not
// match with the document.
var ErrorInvalidCAS = errors.New("invalidCAS")

// ErrorRollback for transactions.
var ErrorRollback = errors.New("rollback")
