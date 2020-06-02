package util

import (
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
func NewLogger(level logLevel) Logger {
	logger := log.New(os.Stderr, "", log.LstdFlags|log.Lshortfile)
	return Logger{
		level:  level,
		logger: logger,
	}
}

// NewConfiguredLogger returns a logger to stderr using the log level in the environment
func NewConfiguredLogger(conf config.Config) Logger {
	levelStr := strings.ToLower(conf.GetStringOrEmpty("LOG_LEVEL"))
	logLevel := stringToLogLevel(levelStr)
	logger := Logger{
		level:  logLevel,
		logger: log.New(os.Stderr, "", log.LstdFlags|log.Lshortfile),
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
	list := prependLogSlice("[error]", args)
	if l.level >= LogLevelError {
		l.logger.Println(list...)
	}
}

// Errorf logs a formatted error message
func (l Logger) Errorf(format string, args ...interface{}) {
	list := prependLogSlice("[error]", args)
	if l.level >= LogLevelError {
		l.logger.Printf(format, list...)
	}
}

// Warn logs a warn message
func (l Logger) Warn(args ...interface{}) {
	list := prependLogSlice("[warn]", args)
	if l.level >= LogLevelWarn {
		l.logger.Println(list...)
	}
}

// Warnf logs a formatted warn message
func (l Logger) Warnf(format string, args ...interface{}) {
	list := prependLogSlice("[warn]", args)
	if l.level >= LogLevelWarn {
		l.logger.Printf(format, list...)
	}
}

// Info logs an info message
func (l Logger) Info(args ...interface{}) {
	list := prependLogSlice("[info]", args)
	if l.level >= LogLevelInfo {
		l.logger.Println(list...)
	}
}

// Infof logs a formatted info message
func (l Logger) Infof(format string, args ...interface{}) {
	list := prependLogSlice("[info]", args)
	if l.level >= LogLevelInfo {
		l.logger.Printf(format, list...)
	}
}

// Debug logs a debug message
func (l Logger) Debug(args ...interface{}) {
	list := prependLogSlice("[debug]", args)
	if l.level >= LogLevelDebug {
		l.logger.Println(list...)
	}
}

// Debugf logs a formatted debug message
func (l Logger) Debugf(format string, args ...interface{}) {
	list := prependLogSlice("[debug]", args)
	if l.level >= LogLevelDebug {
		l.logger.Printf(format, list...)
	}
}

func prependLogSlice(pre string, sl []interface{}) []interface{} {
	out := []interface{}{pre}
	out = append(out, sl...)
	return out
}
