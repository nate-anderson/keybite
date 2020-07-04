package server

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
