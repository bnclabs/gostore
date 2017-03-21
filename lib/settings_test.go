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
	// test with Settings
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
	// test with map[string]interface{}
	setts5 := map[string]interface{}{"section2.param2": 50}
	setts = setts.Mixin(setts5)
	ref["section2.param2"] = 50
	if !reflect.DeepEqual(ref, setts) {
		t.Fatalf("expected %v, got %v", ref, setts)
	}
}

func TestSettingsBool(t *testing.T) {
	setts := Settings{"param1": true, "param2": false, "notbool": 20}
	if v := setts.Bool("param1"); v != true {
		t.Fatalf("expected %v, got %v", true, v)
	} else if v := setts.Bool("param2"); v != false {
		t.Fatalf("expected %v, got %v", false, v)
	}
	// negative cases
	panictest := func(key string) {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("expected panic")
			}
		}()
		setts.Bool(key)
	}
	panictest("invalidparam")
	panictest("notbool")
}

func TestSettingsInt64(t *testing.T) {
	setts := Settings{
		"float64": float64(10), "float32": float32(10),
		"uint": uint(10), "uint64": uint64(10), "uint32": uint32(10),
		"uint16": uint16(10), "uint8": uint8(10),
		"int": int(10), "int64": int64(10), "int32": int32(10),
		"int16": int16(10), "int8": int8(10), "notnumber": true,
	}
	ref := int64(10)
	for key := range setts {
		if key == "notnumber" {
			continue
		}
		if v := setts.Int64(key); v != ref {
			t.Fatalf("for key %v, expected %v, got %v", key, ref, v)
		}
	}
	// negative cases
	panictest := func(key string) {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("expected panic")
			}
		}()
		setts.Int64(key)
	}
	panictest("invalidparam")
	panictest("notnumber")
}

func TestSettingsUint64(t *testing.T) {
	setts := Settings{
		"float64": float64(10), "float32": float32(10),
		"uint": uint(10), "uint64": uint64(10), "uint32": uint32(10),
		"uint16": uint16(10), "uint8": uint8(10),
		"int": int(10), "int64": int64(10), "int32": int32(10),
		"int16": int16(10), "int8": int8(10), "notnumber": true,
	}
	ref := uint64(10)
	for key := range setts {
		if key == "notnumber" {
			continue
		}
		if v := setts.Uint64(key); v != ref {
			t.Fatalf("for key %v, expected %v, got %v", key, ref, v)
		}
	}
	// negative cases
	panictest := func(key string) {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("expected panic")
			}
		}()
		setts.Uint64(key)
	}
	panictest("invalidparam")
	panictest("notnumber")
}

func TestSettingsFloat64(t *testing.T) {
	setts := Settings{
		"float64": float64(10), "float32": float32(10),
		"uint": uint(10), "uint64": uint64(10), "uint32": uint32(10),
		"uint16": uint16(10), "uint8": uint8(10),
		"int": int(10), "int64": int64(10), "int32": int32(10),
		"int16": int16(10), "int8": int8(10), "notnumber": true,
	}
	ref := float64(10)
	for key := range setts {
		if key == "notnumber" {
			continue
		}
		if v := setts.Float64(key); v != ref {
			t.Fatalf("for key %v, expected %v, got %v", key, ref, v)
		}
	}
	// negative cases
	panictest := func(key string) {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("expected panic")
			}
		}()
		setts.Float64(key)
	}
	panictest("invalidparam")
	panictest("notnumber")
}

func TestSettingsString(t *testing.T) {
	setts := Settings{"param": "value", "notstr": false}
	if v := setts.String("param"); v != "value" {
		t.Fatalf("for key %v, expected %v, got %v", "param", "value", v)
	}
	// negative case.
	panictest := func(key string) {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("expected panic")
			}
		}()
		setts.String(key)
	}
	panictest("invalidparam")
	panictest("notstr")
}
