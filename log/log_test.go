package log

import "testing"
import "fmt"
import "os"
import "strings"
import "io/ioutil"

var _ = fmt.Sprintf("dummy")

func TestSetLogger(t *testing.T) {
	logfile := "setlogger_test.log.file"
	logline := "hello world"
	defer os.Remove(logfile)

	ref := &defaultLogger{level: logLevelIgnore, output: nil}
	log := SetLogger(ref, nil).(*defaultLogger)
	if log.level != logLevelIgnore || log.output != nil {
		t.Errorf("expected %v, got %v", ref, log)
	}

	// test a custom logger
	setts := map[string]interface{}{
		"log.level": "info",
		"log.file":  logfile,
	}
	clog := SetLogger(nil, setts)
	clog.Infof(logline)
	clog.Verbosef(logline)
	clog.Fatalf(logline)
	clog.Errorf(logline)
	clog.Warnf(logline)
	clog.Tracef(logline)
	if data, err := ioutil.ReadFile(logfile); err != nil {
		t.Error(err)
	} else if s := string(data); !strings.Contains(s, "hello world") {
		t.Errorf("expected %v, got %v", logline, s)
	} else if len(strings.Split(s, "\n")) != 1 {
		t.Errorf("expected %v, got %v", logline, s)
	}
}

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

func TestLogLevelSettings(t *testing.T) {
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
