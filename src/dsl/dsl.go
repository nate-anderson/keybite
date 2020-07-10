package dsl

import (
	"fmt"
	"keybite/config"
	"keybite/store"
	"strconv"
	"strings"
)

// Execute a statement on the data in the provided datadir
func Execute(input string, conf config.Config) (store.Result, error) {
	action := getAction(input)

	for _, command := range Commands {
		if action == command.keyword {
			tokens, payload, err := getTokensUntil(input, command.numTokens)
			if err != nil {
				return store.EmptyResult(), err
			}
			res, err := command.execute(tokens, payload, conf)
			return res, err
		}
	}
	return store.EmptyResult(), fmt.Errorf("'%s' is not a valid query command", action)
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

// ParseSelector parses a string into a selector. Acceptable formats are 6, [6:10], [6, 7, 8]
func ParseSelector(token string) (store.Selector, error) {
	if token[0] == '[' {
		body := StripBrackets(token)
		// array
		if strings.Contains(body, ",") {
			collection, err := parseCollection(body)
			if err != nil {
				return store.EmptySelector(), err
			}
			selector := store.NewArraySelector(collection)
			return &selector, err
		}
		// range
		if strings.Contains(body, ":") {
			min, max, err := parseRange(body)
			if err != nil {
				return store.EmptySelector(), err
			}
			selector := store.NewRangeSelector(min, max)
			return &selector, nil
		}
	}

	selected, err := strconv.ParseUint(token, 10, 64)
	selector := store.NewSingleSelector(selected)
	return &selector, err
}

// StripBrackets removes surrounding square brackets
func StripBrackets(token string) string {
	return strings.TrimPrefix(
		strings.TrimSuffix(token, "]"),
		"[",
	)
}

// [6,7,8]
func parseCollection(token string) ([]uint64, error) {
	strs := strings.Split(token, ",")
	vals := make([]uint64, len(strs))
	for i, str := range strs {
		id, err := strconv.ParseUint(str, 10, 64)
		if err != nil {
			return vals, err
		}
		vals[i] = id
	}

	return vals, nil
}

// [1:3]
func parseRange(token string) (min uint64, max uint64, err error) {
	parts := strings.Split(token, ":")
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("invalid range selection: must specify min:max")
	}
	min, err = strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid range selection: min and max must be positive integers")
	}
	max, err = strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid range selection: min and max must be positive integers")
	}
	if max < min {
		return 0, 0, fmt.Errorf("invalid range: max must be >= min")
	}
	return
}
