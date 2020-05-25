package server

import (
	"context"
	"errors"
	"fmt"
	"keybite-http/config"
	"keybite-http/dsl"
	"log"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/iancoleman/orderedmap"
)

// HandleLambdaRequest handles a lambda request
func (l LambdaHandler) HandleLambdaRequest(ctx context.Context, payload orderedmap.OrderedMap) (map[string]string, error) {
	queryResults := map[string]string{}
	queries := payload.Keys()

	for _, key := range queries {

		query, ok := payload.Get(key)
		if !ok {
			return map[string]string{}, errors.New("something really broke")
		}

		queryVariables := extractQueryVariables(query.(string))
		log.Println("Query variables: ", queryVariables)
		log.Println("Result map: ", queryResults)
		if len(queryVariables) > 0 && mapHasKeys(queryResults, queryVariables) {
			queryFormat := queryWithVariablesToFormat(query.(string))
			log.Println("variable query format:", queryFormat)
			variableValues := getMapValues(queryResults, queryVariables)
			query = fmt.Sprintf(queryFormat, variableValues...)
			log.Println("Query:", query)
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

type LambdaHandler struct {
	conf config.Config
}

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
