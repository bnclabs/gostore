package lib

import "strings"

type Config map[string]interface{}

// SectionConfig will create a new config object with parameters
// starting with `prefix`.
func (config Config) Section(prefix string) Config {
	section := make(Config)
	for key, value := range config {
		if strings.HasPrefix(key, prefix) {
			section[key] = value
		}
	}
	return section
}

// Trim config parameter with `prefix` string.
func (config Config) Trim(prefix string) Config {
	trimmed := make(Config)
	for key, value := range config {
		trimmed[strings.TrimPrefix(key, prefix)] = value
	}
	return trimmed
}

// Filter config paramters that contain `subs`.
func (config Config) Filter(subs string) Config {
	subconfig := make(Config)
	for key, value := range config {
		if strings.Contains(key, subs) {
			subconfig[key] = value
		}
	}
	return subconfig
}

func (c Config) Bool(key string) bool {
	if value, ok := c[key]; !ok {
		panicerr("missing config %q", key)
	} else if val, ok := value.(bool); !ok {
		panicerr("config %q not a bool: %T", key, value)
	} else {
		return val
	}
	panic("unreachable code")
}

func (c Config) Int64(key string) int64 {
	value, ok := c[key]
	if !ok {
		panicerr("missing config %q", key)
	}
	switch val := value.(type) {
	case float64:
		return int64(val)
	case float32:
		return int64(val)
	case uint:
		return int64(val)
	case uint64:
		return int64(val)
	case uint32:
		return int64(val)
	case uint16:
		return int64(val)
	case uint8:
		return int64(val)
	case int:
		return int64(val)
	case int64:
		return int64(val)
	case int32:
		return int64(val)
	case int16:
		return int64(val)
	case int8:
		return int64(val)
	}
	panicerr("config %v not a number: %T", key, value)
	return 0
}

func (c Config) Uint64(key string) uint64 {
	value, ok := c[key]
	if !ok {
		panicerr("missing config %q", key)
	}
	switch val := value.(type) {
	case float64:
		return uint64(val)
	case float32:
		return uint64(val)
	case uint:
		return uint64(val)
	case uint64:
		return uint64(val)
	case uint32:
		return uint64(val)
	case uint16:
		return uint64(val)
	case uint8:
		return uint64(val)
	case int:
		return uint64(val)
	case int64:
		return uint64(val)
	case int32:
		return uint64(val)
	case int16:
		return uint64(val)
	case int8:
		return uint64(val)
	}
	panicerr("config %v not a number: %T", key, value)
	return 0
}

func (c Config) String(key string) string {
	if value, ok := c[key]; !ok {
		panicerr("missing config %q", key)
	} else if val, ok := value.(string); !ok {
		panicerr("config %v not a number: %T", key, value)
	} else {
		return val
	}
	panic("unreachable code")
}

func Mixinconfig(configs ...interface{}) Config {
	update := func(dst Config, config map[string]interface{}) Config {
		for key, value := range config {
			dst[key] = value
		}
		return dst
	}
	dst := make(Config)
	for _, config := range configs {
		switch cnf := config.(type) {
		case Config:
			dst = update(dst, map[string]interface{}(cnf))
		case map[string]interface{}:
			dst = update(dst, cnf)
		}
	}
	return dst
}
