package dsl

import (
	"fmt"
	"keybite-http/config"
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
			return command.execute(tokens, payload, conf)
		}
	}
	return "", fmt.Errorf("%s is not a valid query command", action)
}

func getTokensUntil(s string, until int) (tokens []string, remaining string, err error) {
	fields := strings.Fields(s)

	if len(fields) < until {
		err = fmt.Errorf("malformed query: minimum length of %d tokens not met", (until + 1))
		return
	}
	tokens = fields[0 : until+1]
	remaining = strings.Join(fields[until:], " ")
	return
}

// extract the action keyword from a query
func getAction(q string) string {
	return strings.ToLower(
		strings.Fields(q)[0],
	)
}
