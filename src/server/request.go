package server

import (
	"fmt"
	"keybite/config"
	"keybite/util/log"
	"strings"
)

// NoResultWantedPrefix should be used in JSON queries to indicate that no response value is desired, but the query should still be executed
const NoResultWantedPrefix = "_"

// Request is a mapped collection of requests marshalled from JSON
type Request map[string]*Query

// LinkQueryDependencies populates the `deps` field of each request query based on the other queries
func (r *Request) LinkQueryDependencies() error {
	for _, q := range *r {
		err := q.LinkDependencies(*r)
		if err != nil {
			return err
		}
	}
	return nil
}

// ExecuteQueries executes all queries in the request
// Query dependency pointers must be set before calling this function
func (r Request) ExecuteQueries(conf config.Config) ResultSet {
	results := make(ResultSet, len(r))
	for key, query := range r {
		result, err := query.Execute(conf, results)
		if err != nil {
			log.Infof("error executing query DSL: %s", err.Error())
			results[key] = NullableString{}
			continue
		}
		results[key] = toNullableString(result)
	}

	return results
}

type keyList []string

func (l keyList) contains(key string) bool {
	for _, member := range l {
		if member == key {
			return true
		}
	}
	return false
}

// ResolveQuery resolves a query into a resultset
func ResolveQuery(key string, q Query, conf config.Config, results ResultSet, seen keyList) error {
	// resolve q's deps
	for i, dep := range q.deps {
		depKey := q.depVars[i]
		if !results.HasKey(depKey) {
			if seen.contains(depKey) {
				return fmt.Errorf("circular dependency on variable '%s'", depKey)
			}
			err := ResolveQuery(depKey, *dep, conf, results, seen)
			if err != nil {
				return err
			}
		}
	}

	// resolve q
	res, err := q.Execute(conf, results)
	if err != nil {
		results[key] = NullableString{}
		return err
	}

	if !strings.HasPrefix(key, NoResultWantedPrefix) {
		results[key] = toNullableString(res)
	}

	seen = append(seen, key)
	return nil
}
