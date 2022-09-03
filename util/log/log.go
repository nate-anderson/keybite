package log

import (
	"io"
	"log"
	"os"
	"strings"
)

type logLevel int

const (
	// LevelError allows only error logs to be displayed
	LevelError logLevel = iota
	// LevelWarn shows potential problems
	LevelWarn
	// LevelInfo shows info-level logs
	LevelInfo
	// LevelDebug shows debugging values
	LevelDebug
)

var level = LevelError
var writer io.Writer = os.Stdout
var logger = log.New(writer, "", log.LstdFlags)

// SetLevel sets the log package log level
func SetLevel(newLevel logLevel) {
	level = newLevel
}

// SetLevelString allows setting log level with a string (like from config)
func SetLevelString(levelStr string) {
	newLevel := stringToLogLevel(levelStr)
	SetLevel(newLevel)
}

// SetWriter sets the writer for output from this package
func SetWriter(newWriter io.Writer) {
	writer = newWriter
}

func stringToLogLevel(str string) logLevel {
	levelStr := strings.ToLower(str)
	switch levelStr {
	case "error":
		return LevelError
	case "warn":
		return LevelWarn
	case "info":
		return LevelInfo
	case "debug":
		return LevelDebug
	default:
		log.Printf("Invalid log level string '%s'", str)
		return LevelWarn
	}
}

// Always logs the provided message regardless of log level
func Always(args ...any) {
	logger.Println(args...)
}

// Alwaysf logs the provided formatted message regardless of log level
func Alwaysf(format string, args ...any) {
	logger.Printf(format, args...)
}

// Error logs an error message
func Error(args ...any) {
	if level >= LevelError {
		logger.Println(args...)
	}
}

// Errorf logs a formatted error message
func Errorf(format string, args ...any) {
	sformat := PrependString("[ERROR]", format)
	if level >= LevelError {
		logger.Printf(sformat, args...)
	}
}

// Warn logs a warn message
func Warn(args ...any) {
	if level >= LevelWarn {
		logger.Println(args...)
	}
}

// Warnf logs a formatted warn message
func Warnf(format string, args ...any) {
	sformat := PrependString("[WARN]", format)
	if level >= LevelWarn {
		logger.Printf(sformat, args...)
	}
}

// Info logs an info message
func Info(args ...any) {
	if level >= LevelInfo {
		logger.Println(args...)
	}
}

// Infof logs a formatted info message
func Infof(format string, args ...any) {
	sformat := PrependString("[INFO]", format)
	if level >= LevelInfo {
		logger.Printf(sformat, args...)
	}
}

// Debug logs a debug message
func Debug(args ...any) {
	if level >= LevelDebug {
		logger.Println(args...)
	}
}

// Debugf logs a formatted debug message
func Debugf(format string, args ...any) {
	sformat := PrependString("[DEBUG]", format)
	if level >= LevelDebug {
		logger.Printf(sformat, args...)
	}
}

// PrependString prepends a prefix to a string
func PrependString(pre string, str string) string {
	return (pre + " " + str)
}
