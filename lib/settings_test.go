package lib

import "testing"
import "fmt"
import "reflect"

var _ = fmt.Sprintf("dummy")

func TestSettingsSection(t *testing.T) {
	setts := Settings{
		"section1.param1": 10,
		"section1.param2": 20,
		"section2.param1": 30,
		"section2.param2": 40,
	}
	ref := Settings{
		"section1.param1": 10,
		"section1.param2": 20,
	}
	section := setts.Section("section1")
	if !reflect.DeepEqual(ref, section) {
		t.Fatalf("expected %v, got %v", ref, section)
	}
}

func TestSettingsTrim(t *testing.T) {
	setts := Settings{
		"section1.param1": 10,
		"section1.param2": 20,
		"section2.param1": 30,
		"section2.param2": 40,
	}
	ref := Settings{
		"param1": 10,
		"param2": 20,
	}
	trimmed := setts.Section("section1").Trim("section1.")
	if !reflect.DeepEqual(ref, trimmed) {
		t.Fatalf("expected %v, got %v", ref, trimmed)
	}
}

func TestSettingsFilter(t *testing.T) {
	setts := Settings{
		"section1.param1": 10,
		"section1.param2": 20,
		"section2.param1": 30,
		"section2.param2": 40,
	}
	ref := Settings{
		"section1.param1": 10,
		"section2.param1": 30,
	}
	filtered := setts.Filter("param1")
	if !reflect.DeepEqual(ref, filtered) {
		t.Fatalf("expected %v, got %v", ref, filtered)
	}
}

func TestSettingsMixin(t *testing.T) {
	setts1 := Settings{"section1.param1": 10}
	setts2 := Settings{"section1.param2": 20}
	setts3 := Settings{"section2.param1": 30}
	setts4 := Settings{"section2.param2": 40}
	setts := make(Settings).Mixin(setts1, setts2, setts3, setts4)
	ref := Settings{
		"section1.param1": 10,
		"section1.param2": 20,
		"section2.param1": 30,
		"section2.param2": 40,
	}
	if !reflect.DeepEqual(ref, setts) {
		t.Fatalf("expected %v, got %v", ref, setts)
	}
}

func TestSettingsBool(t *testing.T) {
	setts := Settings{"param1": true, "param2": false}
	if v := setts.Bool("param1"); v != true {
		t.Fatalf("expected %v, got %v", true, v)
	} else if v := setts.Bool("param2"); v != false {
		t.Fatalf("expected %v, got %v", false, v)
	}
}

func TestSettingsInt64(t *testing.T) {
	setts := Settings{
		"float64": float64(10), "float32": float32(10),
		"uint": int64(10), "uint64": int64(10), "uint32": int64(10),
		"uint16": int64(10), "uint8": int64(10),
		"int": int64(10), "int64": int64(10), "int32": int64(10),
		"int16": int64(10), "int8": int64(10),
	}
	ref := int64(10)
	for key := range setts {
		if v := setts.Int64(key); v != ref {
			t.Fatalf("for key %v, expected %v, got %v", key, ref, v)
		}
	}
}

func TestSettingsUint64(t *testing.T) {
	setts := Settings{
		"float64": float64(10), "float32": float32(10),
		"uint": int64(10), "uint64": int64(10), "uint32": int64(10),
		"uint16": int64(10), "uint8": int64(10),
		"int": int64(10), "int64": int64(10), "int32": int64(10),
		"int16": int64(10), "int8": int64(10),
	}
	ref := uint64(10)
	for key := range setts {
		if v := setts.Uint64(key); v != ref {
			t.Fatalf("for key %v, expected %v, got %v", key, ref, v)
		}
	}
}

func TestSettingsString(t *testing.T) {
	setts := Settings{"param": "value"}
	if v := setts.String("param"); v != "value" {
		t.Fatalf("for key %v, expected %v, got %v", "param", "value", v)
	}
}
