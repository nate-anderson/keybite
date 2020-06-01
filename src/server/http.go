package server

import (
	"encoding/json"
	"fmt"
	"keybite-http/config"
	"keybite-http/dsl"
	"net/http"
	"regexp"

	"github.com/iancoleman/orderedmap"
)

// NoResultWantedKey should be used in JSON queries to indicate that no response value is desired, but the query should still be executed
const NoResultWantedKey = "_"

// StartHTTPServer starts the HTTP server
func StartHTTPServer(conf config.Config) error {
	port, err := conf.GetString("HTTP_PORT")
	if err != nil {
		return err
	}

	fmt.Printf("Serving on port %s\n", port)

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
	queryList := orderedmap.New()
	decoder := json.NewDecoder(req.Body)
	err := decoder.Decode(&queryList)
	if err != nil {
		errText := fmt.Sprintf("could not parse request JSON: %s", err.Error())
		http.Error(w, errText, http.StatusBadRequest)
		return
	}

	queryResults := map[string]string{}
	queries := queryList.Keys()

	for _, key := range queries {

		query, ok := queryList.Get(key)
		if !ok {
			http.Error(w, "something really broke", http.StatusInternalServerError)
			return
		}

		queryVariables := extractQueryVariables(query.(string))
		if len(queryVariables) > 0 && mapHasKeys(queryResults, queryVariables) {
			queryFormat := queryWithVariablesToFormat(query.(string))
			variableValues := getMapValues(queryResults, queryVariables)
			query = fmt.Sprintf(queryFormat, variableValues...)
		}

		result, err := dsl.Execute(query.(string), h.conf)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		// if key == "_", don't add it to the return value
		if key == NoResultWantedKey {
			continue
		}

		queryResults[key] = result
	}

	respond(w, queryResults, http.StatusOK)

}

func respond(w http.ResponseWriter, data interface{}, status int) {
	w.WriteHeader(status)
	resBytes, err := json.Marshal(data)
	if err != nil {
		http.Error(w, "Error marshaling response body", http.StatusInternalServerError)
		return
	}

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

// does the map contain all of the provided keys
func mapHasKeys(m map[string]string, keys []string) bool {
	for _, key := range keys {
		if _, ok := m[key]; !ok {
			return false
		}
	}
	return true
}

// get a collection of map values from a collection of keys
func getMapValues(m map[string]string, keys []string) []interface{} {
	res := []string{}
	for _, key := range keys {
		res = append(res, m[key])
	}
	return strSliceToInterfaceSlice(res)
}

func strSliceToInterfaceSlice(strSlice []string) []interface{} {
	new := make([]interface{}, len(strSlice))
	for i, v := range strSlice {
		new[i] = v
	}
	return new
}
