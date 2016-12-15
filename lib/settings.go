package lib

import "strings"

// Settings map of settings parameters.
type Settings map[string]interface{}

// Section will create a new settings object with parameters
// starting with `prefix`.
func (setts Settings) Section(prefix string) Settings {
	section := make(Settings)
	for key, value := range setts {
		if strings.HasPrefix(key, prefix) {
			section[key] = value
		}
	}
	return section
}

// Trim settings parameter with `prefix` string.
func (setts Settings) Trim(prefix string) Settings {
	trimmed := make(Settings)
	for key, value := range setts {
		trimmed[strings.TrimPrefix(key, prefix)] = value
	}
	return trimmed
}

// Filter settings paramters that contain `subs`.
func (setts Settings) Filter(subs string) Settings {
	subsetts := make(Settings)
	for key, value := range setts {
		if strings.Contains(key, subs) {
			subsetts[key] = value
		}
	}
	return subsetts
}

// Mixin settings to override `setts` with `settings`.
func (setts Settings) Mixin(settings ...interface{}) Settings {
	update := func(arg map[string]interface{}) {
		for key, value := range arg {
			setts[key] = value
		}
	}
	for _, arg := range settings {
		switch cnf := arg.(type) {
		case Settings:
			update(map[string]interface{}(cnf))
		case map[string]interface{}:
			update(cnf)
		}
	}
	return setts
}

// Bool return the boolean value for key.
func (setts Settings) Bool(key string) bool {
	if value, ok := setts[key]; !ok {
		panicerr("missing settings %q", key)
	} else if val, ok := value.(bool); !ok {
		panicerr("settings %q not a bool: %T", key, value)
	} else {
		return val
	}
	panic("unreachable code")
}

// Float64 return the int64 value for key.
func (setts Settings) Float64(key string) float64 {
	value, ok := setts[key]
	if !ok {
		panicerr("missing settings %q", key)
	}
	switch val := value.(type) {
	case float64:
		return val
	case float32:
		return float64(val)
	case uint:
		return float64(val)
	case uint64:
		return float64(val)
	case uint32:
		return float64(val)
	case uint16:
		return float64(val)
	case uint8:
		return float64(val)
	case int:
		return float64(val)
	case int64:
		return float64(val)
	case int32:
		return float64(val)
	case int16:
		return float64(val)
	case int8:
		return float64(val)
	}
	panicerr("settings %v not a number: %T", key, value)
	return 0
}

// Int64 return the int64 value for key.
func (setts Settings) Int64(key string) int64 {
	value, ok := setts[key]
	if !ok {
		panicerr("missing settings %q", key)
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
	panicerr("settings %v not a number: %T", key, value)
	return 0
}

// Uint64 return the uint64 value for key.
func (setts Settings) Uint64(key string) uint64 {
	value, ok := setts[key]
	if !ok {
		panicerr("missing settings %q", key)
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
	panicerr("settings %v not a number: %T", key, value)
	return 0
}

// String return the string value for key.
func (setts Settings) String(key string) string {
	if value, ok := setts[key]; !ok {
		panicerr("missing settings %q", key)
	} else if val, ok := value.(string); !ok {
		panicerr("settings %v not a number: %T", key, value)
	} else {
		return val
	}
	panic("unreachable code")
}
