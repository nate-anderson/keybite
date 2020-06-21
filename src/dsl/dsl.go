package dsl

import (
	"fmt"
	"keybite/config"
	"strings"
)

// Execute a statement on the data in the provided datadir
func Execute(input string, conf config.Config) (string, error) {
	action := getAction(input)

	for _, command := range Commands {
		if action == command.keyword {
			tokens, payload, err := getTokensUntil(input, command.numTokens)
			if err != nil {
				return "", err
			}
			res, err := command.execute(tokens, payload, conf)
			return res, err
		}
	}
	return "", fmt.Errorf("'%s' is not a valid query command", action)
}

func getTokensUntil(s string, until int) (tokens []string, payload string, err error) {
	fields := strings.Fields(s)

	if len(fields) < until {
		err = fmt.Errorf("malformed query: minimum length of %d tokens not met", (until + 1))
		return
	}
	tokens = fields[0 : until+1]
	payload = strings.Join(fields[until:], " ")
	return
}

// extract the action keyword from a query
func getAction(q string) string {
	tokens := strings.Fields(q)
	if len(tokens) == 0 {
		return ""
	}

	return strings.ToLower(
		tokens[0],
	)
}

func displayCommandList() {
	fmt.Println("Available query commands: ")
	for _, command := range Commands {
		fmt.Println(command.keyword)
		fmt.Println("  ", command.description)
		fmt.Println("  Example:", command.example)
		fmt.Println()
	}
}
