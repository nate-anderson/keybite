package dsl

import (
	"fmt"
	"strings"
)

// Error is a DSL syntax/parsing error
type Error struct {
	InternalError   error
	Message         string
	RemainingTokens []string
}

const maxSnippetTokens = 5

func (e Error) Error() string {
	return fmt.Sprintf("Syntax error at '%s': %s", e.makeSnippet(), e.Message)
}

func (e Error) makeSnippet() string {
	tokens := limit(e.RemainingTokens, maxSnippetTokens)
	if len(tokens) > maxSnippetTokens {
		tokens = append(tokens, "...")
	}
	return strings.Join(tokens, " ")
}

func syntaxError(badToken string, remaining []string, message string) error {
	return Error{
		Message:         message,
		RemainingTokens: remaining,
	}
}

func parsingError(badToken string, remaining []string, message string, err error) error {
	return Error{
		Message:         message,
		RemainingTokens: remaining,
		InternalError:   err,
	}
}

func unexpectedEndOfInputError(rawInput, expectedTokenDescription string) error {
	return Error{
		RemainingTokens: []string{rawInput + "[!]"},
		Message:         fmt.Sprintf("expected %s", expectedTokenDescription),
	}
}

func limit(strs []string, limit int) []string {
	if len(strs) <= limit {
		return strs
	}
	return strs[:(limit - 1)]
}
