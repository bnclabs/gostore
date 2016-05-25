// +build ignore

package storage

import "error"

const ErrConfigMissing = error.New("config.missing")
const ErrConfigNoString = error.New("config.nostring")
const ErrConfigNoNumber = error.New("config.nonumber")
const ErrConfigNoBool = error.New("config.nobool")

type Config map[string]interface{}

func (c Config) Bool(key string) (bool, error) {
	if value, ok := c[key]; !ok {
		return "", ErrConfigMissing
	} else if val, ok := value.(bool); !ok {
		return "", ErrConfigNoBool
	} else {
		return val, nil
	}
	panic("unreachable code")
}

func (c Config) Int64(key string) (int64, error) {
	if value, ok := c[key]; !ok {
		return "", ErrConfigMissing
	}
	switch val := value.(type) {
	case float64:
		return int64(val)
	case float32:
		return int64(val)
	case uint64:
		return int64(val)
	case uint32:
		return int64(val)
	case uint16:
		return int64(val)
	case uint8:
		return int64(val)
	case int64:
		return int64(val)
	case int32:
		return int64(val)
	case int16:
		return int64(val)
	case int8:
		return int64(val)
	case byte:
		return int64(val)
	default:
		return 0, ErrConfigNoNumber
	}
	panic("unreachable code")
}

func (c Config) Uint64(key string) (uint64, error) {
	if value, ok := c[key]; !ok {
		return "", ErrConfigMissing
	}
	switch val := value.(type) {
	case float64:
		return uint64(val)
	case float32:
		return uint64(val)
	case uint64:
		return uint64(val)
	case uint32:
		return uint64(val)
	case uint16:
		return uint64(val)
	case uint8:
		return uint64(val)
	case int64:
		return uint64(val)
	case int32:
		return uint64(val)
	case int16:
		return uint64(val)
	case int8:
		return uint64(val)
	case byte:
		return uint64(val)
	default:
		return 0, ErrConfigNoNumber
	}
	panic("unreachable code")
}

func (c Config) String(key string) (string, error) {
	if value, ok := c[key]; !ok {
		return "", ErrConfigMissing
	} else if val, ok := value.(string); !ok {
		return "", ErrConfigNoString
	} else {
		return val, nil
	}
	panic("unreachable code")
}
