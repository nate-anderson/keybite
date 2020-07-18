package server

import (
	"errors"
	"fmt"
	"keybite/config"
	"keybite/util/log"
	"strings"
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

// LogQueryErrorInfo recursively unwraps and logs errors
// the top-level error is logged at the Info log level, and
// nested errors are logged at the debug log level
func LogQueryErrorInfo(queryKey string, err error) {
	current := err
	indentPrefix := "└──"
	// initial indent
	indentSpaces := 1
	// increase each line's indent by
	indentIncrease := 1
	log.Infof("[ :%s ] %s", queryKey, current.Error())
	for errors.Unwrap(current) != nil {
		current = errors.Unwrap(current)
		indentPrefix := fmt.Sprintf("%s%s", strings.Repeat(" ", indentSpaces), indentPrefix)
		log.Debugf("%s %s", indentPrefix, current.Error())
		indentSpaces += indentIncrease
	}
}
