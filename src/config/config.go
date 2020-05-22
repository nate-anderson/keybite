package config

import (
	"errors"
	"os"
	"strconv"

	"github.com/joho/godotenv"
)

// Config contains the application configuration and reads it as needed from env
type Config map[string]string

// ErrConfigNotFound is returned when a config variable is not defined
var ErrConfigNotFound = errors.New("Undefined configuration variable")

// MakeConfig initializes a configuration management
func MakeConfig() (Config, error) {
	err := godotenv.Load()
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
		return "", ErrConfigNotFound
	}

	c[key] = val
	return val, nil
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
