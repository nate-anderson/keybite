package main

import (
	"flag"
	"fmt"
	"keybite/config"
	"keybite/dsl"
	"keybite/server"
	"keybite/util/log"
	"os"
	"strings"
)

func main() {
	displayHelp := flag.Bool("help", false, "--help")
	flag.Parse()
	if *displayHelp {
		dsl.DisplayHelp()
		return
	}

	conf, err := config.MakeConfig()
	if err != nil {
		fmt.Printf("error loading environment configuration: %s\n", err.Error())
		panic(err)
	}

	logLevel, err := conf.GetString("LOG_LEVEL")
	if err != nil {
		log.Warnf("Invalid log level %s configured", logLevel)
		logLevel = "INFO"
	}

	log.SetLevelString(logLevel)

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
	err = server.StartConfiguredServer(conf)
	if err != nil {
		log.Error(err.Error())
	}

}
