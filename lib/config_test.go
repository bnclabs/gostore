package lib

import "testing"
import "fmt"
import "reflect"

var _ = fmt.Sprintf("dummy")

func TestConfigSection(t *testing.T) {
	config := Config{
		"section1.param1": 10,
		"section1.param2": 20,
		"section2.param1": 30,
		"section2.param2": 40,
	}
	ref := Config{
		"section1.param1": 10,
		"section1.param2": 20,
	}
	section := config.Section("section1")
	if !reflect.DeepEqual(ref, section) {
		t.Fatalf("expected %v, got %v", ref, section)
	}
}

func TestConfigTrim(t *testing.T) {
	config := Config{
		"section1.param1": 10,
		"section1.param2": 20,
		"section2.param1": 30,
		"section2.param2": 40,
	}
	ref := Config{
		"param1": 10,
		"param2": 20,
	}
	trimmed := config.Section("section1").Trim("section1.")
	if !reflect.DeepEqual(ref, trimmed) {
		t.Fatalf("expected %v, got %v", ref, trimmed)
	}
}

func TestConfigFilter(t *testing.T) {
	config := Config{
		"section1.param1": 10,
		"section1.param2": 20,
		"section2.param1": 30,
		"section2.param2": 40,
	}
	ref := Config{
		"section1.param1": 10,
		"section2.param1": 30,
	}
	filtered := config.Filter("param1")
	if !reflect.DeepEqual(ref, filtered) {
		t.Fatalf("expected %v, got %v", ref, filtered)
	}
}

func TestConfigMixin(t *testing.T) {
	config1 := Config{"section1.param1": 10}
	config2 := Config{"section1.param2": 20}
	config3 := Config{"section2.param1": 30}
	config4 := Config{"section2.param2": 40}
	config := make(Config).Mixin(config1, config2, config3, config4)
	ref := Config{
		"section1.param1": 10,
		"section1.param2": 20,
		"section2.param1": 30,
		"section2.param2": 40,
	}
	if !reflect.DeepEqual(ref, config) {
		t.Fatalf("expected %v, got %v", ref, config)
	}
}

func TestConfigBool(t *testing.T) {
	config := Config{"param1": true, "param2": false}
	if v := config.Bool("param1"); v != true {
		t.Fatalf("expected %v, got %v", true, v)
	} else if v := config.Bool("param2"); v != false {
		t.Fatalf("expected %v, got %v", false, v)
	}
}

func TestConfigInt64(t *testing.T) {
	config := Config{
		"float64": float64(10), "float32": float32(10),
		"uint": int64(10), "uint64": int64(10), "uint32": int64(10),
		"uint16": int64(10), "uint8": int64(10),
		"int": int64(10), "int64": int64(10), "int32": int64(10),
		"int16": int64(10), "int8": int64(10),
	}
	ref := int64(10)
	for key := range config {
		if v := config.Int64(key); v != ref {
			t.Fatalf("for key %v, expected %v, got %v", key, ref, v)
		}
	}
}

func TestConfigUint64(t *testing.T) {
	config := Config{
		"float64": float64(10), "float32": float32(10),
		"uint": int64(10), "uint64": int64(10), "uint32": int64(10),
		"uint16": int64(10), "uint8": int64(10),
		"int": int64(10), "int64": int64(10), "int32": int64(10),
		"int16": int64(10), "int8": int64(10),
	}
	ref := uint64(10)
	for key := range config {
		if v := config.Uint64(key); v != ref {
			t.Fatalf("for key %v, expected %v, got %v", key, ref, v)
		}
	}
}

func TestConfigString(t *testing.T) {
	config := Config{"param": "value"}
	if v := config.String("param"); v != "value" {
		t.Fatalf("for key %v, expected %v, got %v", "param", "value", v)
	}
}
