package main

import (
	"fmt"
	"keybite-http/config"
	"keybite-http/dsl"
	"log"
	"os"
	"strings"
)

var dataDir string
var autoPageSize int
var mapPageSize int

func main() {
	conf, err := config.MakeConfig()
	if err != nil {
		fmt.Println("error loading environment configuration")
		panic(err)
	}

	fmt.Println("Keybite v0.0.2")
	if len(os.Args) == 1 {
		help()
		return
	}

	rootCmd := strings.ToLower(os.Args[1])
	input := strings.Join(os.Args[2:], " ")
	switch rootCmd {
	case "serve":
		log.Fatal(startServer(conf))
	case "cli":
		result, err := dsl.Execute(input, conf)
		if err != nil {
			panic(err)
		}
		fmt.Println(result)
	default:
		help()
	}
}

func help() {
	fmt.Println("Try 'cli' to query against the store locally, or 'serve' to serve the HTTP API")
}
