package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/joho/godotenv"
)

// Config contains the application configuration and reads it as needed from env
type Config map[string]string

// ErrConfigNotFound is returned when a config variable is not defined
func ErrConfigNotFound(key string) error {
	return fmt.Errorf("Undefined configuration variable '%s'", key)
}

// MakeConfig initializes a configuration management
func MakeConfig(filenames ...string) (Config, error) {
	env := strings.ToLower(os.Getenv("ENVIRONMENT"))
	// if lambda environment, there is no .env
	if env == "lambda" {
		return Config{}, nil
	}
	err := godotenv.Load(filenames...)
	return Config{}, err
}

// GetString from configuration
func (c Config) GetString(key string) (string, error) {
	val, ok := c[key]
	if ok {
		return val, nil
	}

	val = os.Getenv(key)
	if val == "" {
		return "", ErrConfigNotFound(key)
	}

	c[key] = val
	return val, nil
}

// GetStringOrEmpty returns a configured string or empty string if not configured
func (c Config) GetStringOrEmpty(key string) string {
	val, ok := c[key]
	if ok {
		return val
	}

	val = os.Getenv(key)
	c[key] = val
	return val
}

// GetInt from configuration
func (c Config) GetInt(key string) (int, error) {
	valStr, err := c.GetString(key)
	if err != nil {
		return 0, err
	}

	return strconv.Atoi(valStr)
}

// GetInt64 from configuration
func (c Config) GetInt64(key string) (int64, error) {
	valStr, err := c.GetString(key)
	if err != nil {
		return 0, err
	}

	return strconv.ParseInt(valStr, 10, 64)
}
