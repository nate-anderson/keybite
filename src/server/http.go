package server

import (
	"encoding/json"
	"fmt"
	"keybite/config"
	"keybite/util/log"
	"net/http"
)

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
	request := Request{}
	decoder := json.NewDecoder(req.Body)
	err := decoder.Decode(&request)
	if err != nil {
		log.Infof("%s: client %s JSON request could not be decoded: %s", req.RequestURI, req.RemoteAddr, err.Error())
		errText := "JSON error: could not parse client request. Query object should be a single object with depth 1"
		respondError(w, errText, http.StatusBadRequest)
		return
	}

	err = request.LinkQueryDependencies()
	if err != nil {
		log.Infof("error linking query dependencies: %s", err.Error())
		respondError(w, err.Error(), http.StatusBadRequest)
		return
	}

	queryResults := ResultSet{}
	seen := keyList{}
	for key, query := range request {
		err := ResolveQuery(key, *query, h.conf, queryResults, seen)
		if err != nil {
			LogQueryErrorInfo(key, err)
			continue
		}
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
