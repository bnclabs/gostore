//  Copyright (c) 2014 Couchbase, Inc.

package storage

import "io"
import "os"
import "fmt"
import "time"
import "strings"

// Logger interface for gofast logging, applications can
// supply a logger object implementing this interface or
// gofast will fall back to the DefaultLogger{}.
type Logger interface {
	Fatalf(format string, v ...interface{})
	Errorf(format string, v ...interface{})
	Warnf(format string, v ...interface{})
	Infof(format string, v ...interface{})
	Verbosef(format string, v ...interface{})
	Debugf(format string, v ...interface{})
	Tracef(format string, v ...interface{})
}

// DefaultLogger with default log-file as os.Stdout and,
// default log-level as LogLevelInfo. Applications can
// supply a Logger{} object when instantiating the
// Transport.
type DefaultLogger struct {
	level  logLevel
	output io.Writer
}

type logLevel int

const (
	logLevelIgnore logLevel = iota + 1
	logLevelFatal
	logLevelError
	logLevelWarn
	logLevelInfo
	logLevelVerbose
	logLevelDebug
	logLevelTrace
)

var log Logger // object used by gofast component for logging.

func setLogger(logger Logger, config map[string]interface{}) Logger {
	if logger != nil {
		log = logger
		return log
	}

	var err error
	level := logLevelInfo
	if val, ok := config["log.level"]; ok {
		level = string2logLevel(val.(string))
	}
	logfd := os.Stdout
	if val, ok := config["log.file"]; ok {
		logfile := val.(string)
		logfd, err = os.OpenFile(logfile, os.O_RDWR|os.O_APPEND, 0660)
		if err != nil {
			if logfd, err = os.Create(logfile); err != nil {
				panic(err)
			}
		}
	}
	log = &DefaultLogger{level: level, output: logfd}
	return log
}

func (l *DefaultLogger) Fatalf(format string, v ...interface{}) {
	l.printf(logLevelFatal, format, v...)
}

func (l *DefaultLogger) Errorf(format string, v ...interface{}) {
	l.printf(logLevelError, format, v...)
}

func (l *DefaultLogger) Warnf(format string, v ...interface{}) {
	l.printf(logLevelWarn, format, v...)
}

func (l *DefaultLogger) Infof(format string, v ...interface{}) {
	l.printf(logLevelInfo, format, v...)
}

func (l *DefaultLogger) Verbosef(format string, v ...interface{}) {
	l.printf(logLevelVerbose, format, v...)
}

func (l *DefaultLogger) Debugf(format string, v ...interface{}) {
	l.printf(logLevelDebug, format, v...)
}

func (l *DefaultLogger) Tracef(format string, v ...interface{}) {
	l.printf(logLevelTrace, format, v...)
}

func (l *DefaultLogger) printf(level logLevel, format string, v ...interface{}) {
	if l.canlog(level) {
		ts := time.Now().Format("2006-01-02T15:04:05.999Z-07:00")
		fmt.Fprintf(l.output, ts+" ["+level.String()+"] "+format, v...)
	}
}

func (l *DefaultLogger) canlog(level logLevel) bool {
	if level <= l.level {
		return true
	}
	return false
}

func (l logLevel) String() string {
	switch l {
	case logLevelIgnore:
		return "Ignor"
	case logLevelFatal:
		return "Fatal"
	case logLevelError:
		return "Error"
	case logLevelWarn:
		return "Warng"
	case logLevelInfo:
		return "Infom"
	case logLevelVerbose:
		return "Verbs"
	case logLevelDebug:
		return "Debug"
	case logLevelTrace:
		return "Trace"
	}
	panic("unexpected log level") // should never reach here
}

func string2logLevel(s string) logLevel {
	s = strings.ToLower(s)
	switch s {
	case "ignore":
		return logLevelIgnore
	case "fatal":
		return logLevelFatal
	case "error":
		return logLevelError
	case "warn":
		return logLevelWarn
	case "info":
		return logLevelInfo
	case "verbose":
		return logLevelVerbose
	case "debug":
		return logLevelDebug
	case "trace":
		return logLevelTrace
	}
	panic("unexpected log level") // should never reach here
}
