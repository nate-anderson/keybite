package server

import (
	"errors"
	"fmt"
	"keybite-http/config"
	"keybite-http/dsl"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/iancoleman/orderedmap"
)

// HandleLambdaRequest handles a lambda request
func (l LambdaHandler) HandleLambdaRequest(payload orderedmap.OrderedMap) (map[string]string, error) {
	queryResults := make(map[string]string, len(payload.Keys()))
	queries := payload.Keys()

	for _, key := range queries {

		query, ok := payload.Get(key)
		if !ok {
			return map[string]string{}, errors.New("something really broke")
		}

		queryVariables := extractQueryVariables(query.(string))
		if len(queryVariables) > 0 && mapHasKeys(queryResults, queryVariables) {
			queryFormat := queryWithVariablesToFormat(query.(string))
			variableValues := getMapValues(queryResults, queryVariables)
			query = fmt.Sprintf(queryFormat, variableValues...)
		}

		result, err := dsl.Execute(query.(string), l.conf)
		if err != nil {
			return map[string]string{}, err
		}

		// if key == "_", don't add it to the return value
		if key == NoResultWantedKey {
			continue
		}

		queryResults[key] = result
	}

	return queryResults, nil

}

// LambdaHandler is the struct used for handling lambda requests
type LambdaHandler struct {
	conf config.Config
}

// NewLambdaHandler creates a lambda handler
func NewLambdaHandler(conf config.Config) LambdaHandler {
	return LambdaHandler{
		conf: conf,
	}
}

// ServeLambda serves a lambda request
func ServeLambda(conf config.Config) {
	handler := NewLambdaHandler(conf)
	lambda.Start(handler.HandleLambdaRequest)
}
