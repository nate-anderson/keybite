package main

import (
	"fmt"
	"keybite-http/dsl"
	"os"
	"strconv"
	"strings"

	"github.com/joho/godotenv"
)

var dataDir string
var defaultPageSize int

func main() {
	err := godotenv.Load()
	if err != nil {
		fmt.Println("Error parsing environment")
		panic(err)
	}

	dataDir = os.Getenv("DATA_DIR")
	defaultPageSize, err = strconv.Atoi(os.Getenv("DEFAULT_PAGE_SIZE"))
	if err != nil {
		fmt.Printf("Invalid default index page size from environment: %s\n", os.Getenv("DEFAULT_PAGE_SIZE"))
		return
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
		serve()
	case "cli":
		result, err := dsl.Execute(input, dataDir, defaultPageSize)
		if err != nil {
			panic(err)
		}
		fmt.Println(result)
	default:
		help()
	}
}

func serve() {

}

func help() {
	fmt.Println("Try 'cli' to query against the store locally, or 'serve' to serve the HTTP API")
}
