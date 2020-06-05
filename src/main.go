package main

import (
	"fmt"
	"keybite/config"
	"keybite/dsl"
	"keybite/server"
	"keybite/util"
	"os"
	"strings"
)

var dataDir string
var autoPageSize int
var mapPageSize int

func main() {
	conf, err := config.MakeConfig()
	if err != nil {
		fmt.Printf("error loading environment configuration: %s\n", err.Error())
		panic(err)
	}

	log := util.NewConfiguredLogger(conf, nil)

	// if args are passed to tbe binary, run query and returm output to stdout
	if len(os.Args) > 1 {
		input := strings.Join(os.Args[1:], " ")
		result, err := dsl.Execute(input, conf)
		if err != nil {
			log.Error("error handling CLI request")
			panic(err)
		}
		fmt.Println(result)
		return
	}

	// if no args are passed, start in server mode
	err = server.StartConfiguredServer(conf, log)
	if err != nil {
		log.Error(err.Error())
		displayHelp()
	}

}

func displayHelp() {
	fmt.Println("Keybite v0.0.2")
	fmt.Println("Run binary with query as trailing args to query against the store locally, run without args to start configured server mode")
}
