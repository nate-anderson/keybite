package server

import (
	"errors"
	"fmt"
	"keybite/config"
	"keybite/util/log"
	"strings"
)

// HandleRequest handles a request and returns a resultset or a fatal error
// if the request could not be completed
func HandleRequest(request *Request, conf *config.Config) (ResultSet, error) {
	response := make(ResultSet)
	err := request.LinkQueryDependencies()
	if err != nil {
		fatalErr := fmt.Errorf("error linking query dependencies: %s", err.Error())
		return response, fatalErr
	}

	seen := keyList{}
	for key, query := range *request {
		err := ResolveQuery(key, *query, conf, response, seen)
		if err != nil {
			LogQueryErrorInfo(key, err)
			continue
		}
	}
	return response, nil
}

// StartConfiguredServer starts the appropriate server based on the environment env variable
func StartConfiguredServer(conf *config.Config) error {
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
