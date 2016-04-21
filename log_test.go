package storage

import "testing"
import "fmt"

var _ = fmt.Sprintf("dummy")

func TestLogPrefix(t *testing.T) {
	if ref, s := "Ignor", logLevelIgnore.String(); ref != s {
		t.Errorf("expected %v, got %v", ref, s)
	} else if ref, s = "Fatal", logLevelFatal.String(); ref != s {
		t.Errorf("expected %v, got %v", ref, s)
	} else if ref, s = "Error", logLevelError.String(); ref != s {
		t.Errorf("expected %v, got %v", ref, s)
	} else if ref, s = "Warng", logLevelWarn.String(); ref != s {
		t.Errorf("expected %v, got %v", ref, s)
	} else if ref, s = "Infom", logLevelInfo.String(); ref != s {
		t.Errorf("expected %v, got %v", ref, s)
	} else if ref, s = "Verbs", logLevelVerbose.String(); ref != s {
		t.Errorf("expected %v, got %v", ref, s)
	} else if ref, s = "Debug", logLevelDebug.String(); ref != s {
		t.Errorf("expected %v, got %v", ref, s)
	} else if ref, s = "Trace", logLevelTrace.String(); ref != s {
		t.Errorf("expected %v, got %v", ref, s)
	}
}

func TestLogLevelConfig(t *testing.T) {
	if r, l := logLevelIgnore, string2logLevel("ignore"); r != l {
		t.Errorf("expected %v, got %v", r, l)
	} else if r, l = logLevelFatal, string2logLevel("fatal"); r != l {
		t.Errorf("expected %v, got %v", r, l)
	} else if r, l = logLevelError, string2logLevel("error"); r != l {
		t.Errorf("expected %v, got %v", r, l)
	} else if r, l = logLevelWarn, string2logLevel("warn"); r != l {
		t.Errorf("expected %v, got %v", r, l)
	} else if r, l = logLevelInfo, string2logLevel("info"); r != l {
		t.Errorf("expected %v, got %v", r, l)
	} else if r, l = logLevelVerbose, string2logLevel("verbose"); r != l {
		t.Errorf("expected %v, got %v", r, l)
	} else if r, l = logLevelDebug, string2logLevel("debug"); r != l {
		t.Errorf("expected %v, got %v", r, l)
	} else if r, l = logLevelTrace, string2logLevel("trace"); r != l {
		t.Errorf("expected %v, got %v", r, l)
	}
}
