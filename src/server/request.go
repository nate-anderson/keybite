package server

import (
	"keybite/config"
	"keybite/util/log"
)

// Request is a mapped collection of requests marshalled from JSON
type Request map[string]Query

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

// ResolveQuery resolves a query into a resultset
func ResolveQuery(q Query, results ResultSet) error {
	for _, dep := range q.deps {
		if results.HasKey(dep.)
		err := ResolveQuery(*dep, results)
		if err != nil {
			return err
		}
	}

	ResolveQuery(q)
}
