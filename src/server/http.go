package server

import (
	"encoding/json"
	"fmt"
	"keybite/config"
	"keybite/dsl"
	"keybite/util/log"
	"net/http"
	"regexp"

	"github.com/iancoleman/orderedmap"
)

// NoResultWantedKey should be used in JSON queries to indicate that no response value is desired, but the query should still be executed
const NoResultWantedKey = "_"

// ServeHTTP starts the HTTP server
func ServeHTTP(conf config.Config) error {
	port, err := conf.GetString("HTTP_PORT")
	if err != nil {
		return err
	}

	driverName, err := conf.GetString("DRIVER")
	if err != nil {
		return err
	}

	log.Alwaysf("Starting Keybite HTTP server at %s/keybite using driver '%s'", port, driverName)

	r := http.NewServeMux()
	handler := NewQueryHandler(conf)
	r.Handle("/keybite", handler)

	return http.ListenAndServe(port, r)
}

// QueryHandler handles query HTTP requests
type QueryHandler struct {
	conf config.Config
}

// NewQueryHandler creates a query HTTP handler
func NewQueryHandler(conf config.Config) QueryHandler {
	return QueryHandler{
		conf: conf,
	}
}

func (h QueryHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	log.Debugf("%s => %s", req.RemoteAddr, req.RequestURI)
	queryList := orderedmap.New()
	decoder := json.NewDecoder(req.Body)
	err := decoder.Decode(&queryList)
	if err != nil {
		log.Infof("%s: client %s JSON request could not be decoded: %s", req.RequestURI, req.RemoteAddr, err.Error())
		errText := "JSON error: could not parse client request. Query object should be a single object with depth 1"
		respondError(w, errText, http.StatusBadRequest)
		return
	}

	queries := queryList.Keys()
	queryResults := make(ResultSet, len(queries))

	for _, key := range queries {

		query, ok := queryList.Get(key)
		if !ok {
			log.Warn("unable to Get query from request OrderedMap :: something really broke")
			respondError(w, "error retrieving previously cached query result", http.StatusInternalServerError)
			return
		}

		queryVariables := extractQueryVariables(query.(string))
		if len(queryVariables) > 0 && resultSetHasKeys(queryResults, queryVariables) {
			log.Debugf("query contained variables %v", queryVariables)
			queryFormat := queryWithVariablesToFormat(query.(string))
			variableValues := getResultSetValues(queryResults, queryVariables)
			query = fmt.Sprintf(queryFormat, variableValues...)
			log.Debugf("formatted query: '%s'", query)
		}

		result, err := dsl.Execute(query.(string), h.conf)
		if err != nil {
			log.Infof("error executing query DSL: %s", err.Error())
			queryResults[key] = NullableString{}
			continue
		}

		// if key == "_", don't add it to the return value
		if key == NoResultWantedKey {
			continue
		}

		queryResults[key] = toNullableString(result)
	}

	log.Debugf("%s <= %s", req.RemoteAddr, req.RequestURI)
	respond(w, queryResults, http.StatusOK)

}

func respond(w http.ResponseWriter, data interface{}, status int) {
	w.Header().Set("Content-Type", "application/json")
	resBytes, err := json.Marshal(data)
	if err != nil {
		errString := "error marshaling JSON response: " + err.Error()
		log.Warn(errString, err)
		respondError(w, errString, http.StatusInternalServerError)
		return
	}

	w.WriteHeader(status)
	fmt.Fprintf(w, string(resBytes))
	return
}

var findVariableRegex = regexp.MustCompile(`\B:\w+`)

// get a list of variables in a query
func extractQueryVariables(query string) []string {
	return stripStringPrefixes(findVariableRegex.FindAllString(query, -1), 1)
}

func stripStringPrefixes(ss []string, n int) []string {
	results := make([]string, len(ss))
	for i, s := range ss {
		results[i] = s[n:]
	}
	return results
}

// convert a query string with variables to a Go format string
func queryWithVariablesToFormat(query string) string {
	return findVariableRegex.ReplaceAllLiteralString(query, "%s")
}

// ErrResponse is used for sending JSON error messages to client
type ErrResponse struct {
	Msg    string `json:"error"`
	Status int    `json:"status"`
}

func respondError(w http.ResponseWriter, errMsg string, status int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	errBody := ErrResponse{
		Msg:    errMsg,
		Status: status,
	}

	errBytes, err := json.Marshal(errBody)

	if err != nil {
		errMsg := "error encoding JSON response to query"
		http.Error(w, errMsg, http.StatusInternalServerError)
		return
	}

	fmt.Fprint(w, string(errBytes))
}
