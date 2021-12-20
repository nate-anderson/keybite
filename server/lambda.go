package server

import (
	"context"
	"keybite/config"
	"keybite/util/log"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-lambda-go/lambdacontext"
)

// HandleLambdaRequest handles a lambda request
func (l λHandler) HandleLambdaRequest(ctx context.Context, request Request) (ResultSet, error) {
	var requestID string
	functionName := lambdacontext.FunctionName
	λctx, ok := lambdacontext.FromContext(ctx)
	if ok {
		requestID = λctx.AwsRequestID
		log.Debugf("%s :: %s => %s", requestID, λctx.Identity.CognitoIdentityID, functionName)
	} else {
		log.Warnf("incomplete log error: failed to extract lambda context")
	}

	queryResults, fatalErr := HandleRequest(&request, l.conf)
	if fatalErr != nil {
		return ResultSet{}, fatalErr
	}

	log.Debugf("%s :: %s <= %s", requestID, λctx.Identity.CognitoIdentityID, functionName)
	return queryResults, nil

}

// λHandler is the struct used for handling lambda requests
type λHandler struct {
	conf *config.Config
}

// newλHandler creates a lambda handler
func newλHandler(conf *config.Config) λHandler {
	return λHandler{
		conf: conf,
	}
}

// Serveλ serves a lambda request
func Serveλ(conf *config.Config) {
	handler := newλHandler(conf)
	lambda.Start(handler.HandleLambdaRequest)
}
