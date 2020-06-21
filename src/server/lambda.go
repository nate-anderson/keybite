package server

import (
	"context"
	"errors"
	"fmt"
	"keybite/config"
	"keybite/dsl"
	"keybite/util/log"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-lambda-go/lambdacontext"
	"github.com/iancoleman/orderedmap"
)

// HandleLambdaRequest handles a lambda request
func (l λHandler) HandleLambdaRequest(ctx context.Context, payload orderedmap.OrderedMap) (map[string]string, error) {
	var requestID string
	functionName := lambdacontext.FunctionName
	λctx, ok := lambdacontext.FromContext(ctx)
	if ok {
		requestID = λctx.AwsRequestID
		log.Debugf("%s :: %s => %s", requestID, λctx.Identity.CognitoIdentityID, functionName)
	} else {
		log.Warnf("incomplete log error: failed to extract lambda context")
	}
	queryResults := make(map[string]string, len(payload.Keys()))
	queries := payload.Keys()

	for _, key := range queries {

		query, ok := payload.Get(key)
		if !ok {
			return map[string]string{}, errors.New("something really broke")
		}

		queryVariables := extractQueryVariables(query.(string))
		if len(queryVariables) > 0 && mapHasKeys(queryResults, queryVariables) {
			log.Debugf("query contained variables %v", queryVariables)
			queryFormat := queryWithVariablesToFormat(query.(string))
			variableValues := getMapValues(queryResults, queryVariables)
			query = fmt.Sprintf(queryFormat, variableValues...)
			log.Debugf("formatted query: '%s'", query)
		}

		result, err := dsl.Execute(query.(string), l.conf)
		if err != nil {
			log.Infof("error executing query DSL: %s", err.Error())
			return map[string]string{}, err
		}

		// if key == "_", don't add it to the return value
		if key == NoResultWantedKey {
			continue
		}

		queryResults[key] = result
	}

	log.Debugf("%s :: %s <= %s", requestID, λctx.Identity.CognitoIdentityID, functionName)
	return queryResults, nil

}

// λHandler is the struct used for handling lambda requests
type λHandler struct {
	conf config.Config
}

// newλHandler creates a lambda handler
func newλHandler(conf config.Config) λHandler {
	return λHandler{
		conf: conf,
	}
}

// Serveλ serves a lambda request
func Serveλ(conf config.Config) {
	handler := newλHandler(conf)
	lambda.Start(handler.HandleLambdaRequest)
}
