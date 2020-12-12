package server

import (
	"fmt"
	"keybite/config"
	"keybite/dsl"
	"keybite/store"
	"regexp"
	"strconv"
)

var findVariableRegex = regexp.MustCompile(`\B:\w+`)

// Query represents a single user query
type Query struct {
	// the raw query
	raw string
	// the query as a format string
	fmt string
	// the names of variables the query depends on
	depVars []string
	// the dependency queries
	deps []*Query
}

// UnmarshalJSON lets a query string be marshalled to a Query
func (q *Query) UnmarshalJSON(data []byte) error {
	rawText := string(data)
	clean, err := strconv.Unquote(rawText)
	if err != nil {
		return err
	}

	q.raw = clean

	if numVars := q.setQueryVars(); numVars > 0 {
		q.setQueryFormat()
	}

	return nil
}

// populate the query's variable name list by parsing the raw query
func (q *Query) setQueryVars() int {
	varMarkers := findVariableRegex.FindAllString(q.raw, -1)
	q.depVars = StripStringPrefixes(varMarkers, 1)
	return len(q.depVars)
}

// set the go format-string for the query
func (q *Query) setQueryFormat() {
	q.fmt = findVariableRegex.ReplaceAllLiteralString(q.raw, "%s")
}

// LinkDependencies populates the query's dependency pointers from a
func (q *Query) LinkDependencies(queries Request) error {
	for _, depVar := range q.depVars {
		dep, ok := queries[depVar]
		if !ok {
			return fmt.Errorf("query depends on variable ':%s' which is not declared in the request", depVar)
		}
		q.deps = append(q.deps, dep)
	}
	return nil
}

// Complete forms a raw query with variables based on previously resolved values
func (q *Query) Complete(list ResultSet) (string, error) {
	if len(q.depVars) == 0 {
		return q.raw, nil
	}

	variableValues := []string{}
	for _, key := range q.depVars {
		// should not need to check for map membership here, this is checked during dependency linking
		if value := list[key]; value.Valid() {
			variableValues = append(variableValues, list[key].String())
		} else {
			return "", fmt.Errorf("failed executing query with variable ':%s': variable not set in request resolution", key)
		}
	}
	// have to convert results to interface slice for fmt.Sprintf to work
	return fmt.Sprintf(q.fmt, strSliceToInterfaceSlice(variableValues)...), nil
}

// Execute the query and get the result
func (q Query) Execute(conf *config.Config, results ResultSet) (store.Result, error) {
	toExecute, err := q.Complete(results)
	if err != nil {
		return store.EmptyResult(), err
	}

	return dsl.Execute(toExecute, conf)
}

// StripStringPrefixes removes n characters from each string in the given slice
func StripStringPrefixes(ss []string, n int) []string {
	results := make([]string, len(ss))
	for i, s := range ss {
		results[i] = s[n:]
	}
	return results
}
