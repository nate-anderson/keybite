package server

import (
	"errors"
	"keybite/config"
	"keybite/util/log"
)

// StartConfiguredServer starts the appropriate server based on the environment env variable
func StartConfiguredServer(conf config.Config) error {
	environment, err := conf.GetString("ENVIRONMENT")
	if err != nil {
		log.Error("error determining environment")
		panic(err)
	}

	switch environment {
	case "linux":
		ServeHTTP(conf)
	case "lambda":
		Serveλ(conf)
	default:
		err := errors.New("ENVIRONMENT not configured :: cannot start application server")
		return err
	}

	return nil
}
