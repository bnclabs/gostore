package storage

import "errors"

var ErrorOutofMemory = errors.New("llrb.outofmemory")
var ErrConfigMissing = errors.New("config.missing")
var ErrConfigNoString = errors.New("config.nostring")
var ErrConfigNoNumber = errors.New("config.nonumber")
var ErrConfigNoBool = errors.New("config.nobool")
