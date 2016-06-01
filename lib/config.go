package lib

import "strings"

// Config map of settings parameter to configuration value.
type Config map[string]interface{}

// Section will create a new config object with parameters
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

// Mixin configuration to override `config` with `configs`.
func (config Config) Mixin(configs ...interface{}) Config {
	update := func(arg map[string]interface{}) {
		for key, value := range arg {
			config[key] = value
		}
	}
	for _, arg := range configs {
		switch cnf := arg.(type) {
		case Config:
			update(map[string]interface{}(cnf))
		case map[string]interface{}:
			update(cnf)
		}
	}
	return config
}

// Bool return the boolean value for key.
func (config Config) Bool(key string) bool {
	if value, ok := config[key]; !ok {
		panicerr("missing config %q", key)
	} else if val, ok := value.(bool); !ok {
		panicerr("config %q not a bool: %T", key, value)
	} else {
		return val
	}
	panic("unreachable code")
}

// Int64 return the int64 value for key.
func (config Config) Int64(key string) int64 {
	value, ok := config[key]
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

// Uint64 return the uint64 value for key.
func (config Config) Uint64(key string) uint64 {
	value, ok := config[key]
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

// String return the string value for key.
func (config Config) String(key string) string {
	if value, ok := config[key]; !ok {
		panicerr("missing config %q", key)
	} else if val, ok := value.(string); !ok {
		panicerr("config %v not a number: %T", key, value)
	} else {
		return val
	}
	panic("unreachable code")
}
