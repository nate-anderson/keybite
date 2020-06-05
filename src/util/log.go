package util

import (
	"io"
	"keybite/config"
	"log"
	"os"
	"strings"
)

type logLevel int

const (
	// LogLevelError allows only error logs to be displayed
	LogLevelError logLevel = iota
	// LogLevelWarn shows potential problems
	LogLevelWarn
	// LogLevelInfo shows info-level logs
	LogLevelInfo
	// LogLevelDebug shows debugging values
	LogLevelDebug
)

// Logger facilitates leveled, organized logging
type Logger struct {
	level  logLevel
	logger *log.Logger
}

// NewLogger returns a new application logger
func NewLogger(level logLevel, to io.Writer) Logger {
	out := to
	// if a nil writer is provided, log to stderr
	if out == nil {
		out = os.Stderr
	}
	logger := log.New(out, "", log.LstdFlags)
	return Logger{
		level:  level,
		logger: logger,
	}
}

// NewConfiguredLogger returns a logger to stderr using the log level in the environment
func NewConfiguredLogger(conf config.Config, to io.Writer) Logger {
	levelStr := strings.ToLower(conf.GetStringOrEmpty("LOG_LEVEL"))
	logLevel := stringToLogLevel(levelStr)
	out := to
	// if a nil writer is provided, log to stderr
	if out == nil {
		out = os.Stderr
	}
	logger := Logger{
		level:  logLevel,
		logger: log.New(out, "", log.LstdFlags),
	}
	return logger
}

func stringToLogLevel(str string) logLevel {
	switch str {
	case "error":
		return LogLevelError
	case "warn":
		return LogLevelWarn
	case "info":
		return LogLevelInfo
	case "debug":
		return LogLevelDebug
	default:
		log.Println("LOG_LEVEL missing or malformed from environment: defaulting to warn level")
		return LogLevelWarn
	}
}

// Error logs an error message
func (l Logger) Error(args ...interface{}) {
	if l.level >= LogLevelError {
		l.logger.Println(args...)
	}
}

// Errorf logs a formatted error message
func (l Logger) Errorf(format string, args ...interface{}) {
	sformat := PrependString("[ERROR]", format)
	if l.level >= LogLevelError {
		l.logger.Printf(sformat, args...)
	}
}

// Warn logs a warn message
func (l Logger) Warn(args ...interface{}) {
	if l.level >= LogLevelWarn {
		l.logger.Println(args...)
	}
}

// Warnf logs a formatted warn message
func (l Logger) Warnf(format string, args ...interface{}) {
	sformat := PrependString("[WARN]", format)
	if l.level >= LogLevelWarn {
		l.logger.Printf(sformat, args...)
	}
}

// Info logs an info message
func (l Logger) Info(args ...interface{}) {
	if l.level >= LogLevelInfo {
		l.logger.Println(args...)
	}
}

// Infof logs a formatted info message
func (l Logger) Infof(format string, args ...interface{}) {
	sformat := PrependString("[INFO]", format)
	if l.level >= LogLevelInfo {
		l.logger.Printf(sformat, args...)
	}
}

// Debug logs a debug message
func (l Logger) Debug(args ...interface{}) {
	if l.level >= LogLevelDebug {
		l.logger.Println(args...)
	}
}

// Debugf logs a formatted debug message
func (l Logger) Debugf(format string, args ...interface{}) {
	sformat := PrependString("[DEBUG]", format)
	if l.level >= LogLevelDebug {
		l.logger.Printf(sformat, args...)
	}
}

// PrependString prepends a prefix to a string
func PrependString(pre string, str string) string {
	return (pre + " " + str)
}
