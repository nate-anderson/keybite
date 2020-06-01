package main

import (
	"fmt"
	"keybite-http/config"
	"keybite-http/dsl"
	"keybite-http/server"
	"keybite-http/util"
	"os"
	"strings"
)

var dataDir string
var autoPageSize int
var mapPageSize int

func main() {
	log := util.NewLogger(util.LogLevelDebug)

	conf, err := config.MakeConfig()
	if err != nil {
		log.Error("error loading environment configuration")
		panic(err)
	}

	if len(os.Args) == 1 {
		log.Info("handling lambda request")
		server.ServeLambda(conf)
		return
	}

	rootCmd := strings.ToLower(os.Args[1])
	input := strings.Join(os.Args[2:], " ")
	switch rootCmd {
	case "serve":
		err := server.StartHTTPServer(conf)
		if err != nil {
			log.Error("error handling lambda request")
			panic(err)
		}
	case "cli":
		result, err := dsl.Execute(input, conf)
		if err != nil {
			log.Error("error handling CLI request")
			panic(err)
		}
		fmt.Println(result)
	default:
		help()
	}
}

func help() {
	fmt.Println("Keybite v0.0.2")
	fmt.Println("Try 'cli' to query against the store locally, or 'serve' to serve the HTTP API")
}
