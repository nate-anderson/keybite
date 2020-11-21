package dsl

import (
	"fmt"
	"keybite/store"
	"keybite/util/log"
	"regexp"
	"strconv"
	"strings"
)

/*
This file implements an LR(0) parser for keybite's Query DSL
The syntax of the DSL is super simple which allows parsing with
a relatively simple state machine
*/

// finite state machine transitions
type step int

const (
	stepInitial step = iota
	stepFinalAutoSelector
	stepFinalMapSelector
	stepFinalIndexName
	stepFinalPayload
	stepFinalOptionalDirection
	stepListOptionalOffsetOrDirection
	stepQueryIndexName
	stepInsertIndexName
	stepQueryKeyIndexName
	stepUpdateInsertKeyMapSelector
	stepUpdateAutoSelector
	stepListOptionalLimitOrDirection
	stepUpdateInsertKeyIndexName
	stepUpdateIndexName
	stepDeleteIndexName
	stepDeleteKeyIndexName
	stepListIndexName
	stepListKeyIndexName
)

type operationType int

const (
	typeQuery operationType = iota
	typeQueryKey
	typeInsert
	typeInsertKey
	typeUpdate
	typeUpdateKey
	typeUpsertKey
	typeDelete
	typeDeleteKey
	typeList
	typeListKey
	typeCount
	typeCountKey
	typeCreateAutoIndex
	typeCreateMapIndex
	typeDropAutoIndex
	typeDropMapIndex
)

// Operation is a query
type Operation struct {
	oType     operationType
	indexName string
	limit     int
	offset    int
	autoSel   store.AutoSelector
	mapSel    store.MapSelector
	payload   string
	listDesc  bool
}

// parser parses DSL into query objects
type parser struct {
	// the head of the parser against the list of tokens
	i int
	// the raw DSL query string
	raw    string
	tokens []string
	// the state of the parser state machine
	nextStep step
}

// the regular expression used to naively split DSL into tokens
var tokenizeExp = regexp.MustCompile(" ")

// newParser constructs a parser
func newParser(dsl string) parser {
	return parser{
		raw:      dsl,
		tokens:   tokenizeExp.Split(dsl, -1),
		nextStep: stepInitial,
	}
}

func (p parser) current() (string, error) {
	if len(p.tokens) > p.i {
		return p.tokens[p.i], nil
	}
	return "", fmt.Errorf("end of input")
}

func (p *parser) increment() {
	p.i++
}

func (p parser) remaining() []string {
	return p.tokens[p.i:]
}

// Parse the provided query
func (p parser) Parse() (o Operation, dslErr error) {
	var err error
	if len(p.tokens) == 0 {
		dslErr = unexpectedEndOfInputError(p.raw, "operation keyword")
		return
	}
	for {
		switch p.nextStep {
		case stepInitial:
			// this loop only runs on inputs with at least one token, so this error can be ignored
			keyword, _ := p.current()
			// determine the query type and expected next token
			switch keyword {
			case "query":
				o.oType = typeQuery
				p.nextStep = stepQueryIndexName

			case "query_key":
				o.oType = typeQueryKey
				p.nextStep = stepQueryKeyIndexName

			case "insert":
				o.oType = typeInsert
				p.nextStep = stepInsertIndexName

			case "insert_key":
				o.oType = typeInsertKey
				p.nextStep = stepUpdateInsertKeyIndexName

			case "update":
				o.oType = typeUpdate
				p.nextStep = stepUpdateIndexName

			case "update_key":
				o.oType = typeUpdateKey
				p.nextStep = stepUpdateInsertKeyIndexName

			case "upsert_key":
				o.oType = typeUpsertKey
				p.nextStep = stepUpdateInsertKeyIndexName

			case "delete":
				o.oType = typeDelete
				p.nextStep = stepDeleteIndexName

			case "delete_key":
				o.oType = typeDeleteKey
				p.nextStep = stepDeleteKeyIndexName

			case "list":
				o.oType = typeList
				p.nextStep = stepListIndexName

			case "list_key":
				o.oType = typeListKey
				p.nextStep = stepListKeyIndexName

			case "count":
				o.oType = typeCount
				p.nextStep = stepFinalIndexName

			case "count_key":
				o.oType = typeCountKey
				p.nextStep = stepFinalIndexName

			case "create_auto_index":
				o.oType = typeCreateAutoIndex
				p.nextStep = stepFinalIndexName

			case "create_map_index":
				o.oType = typeCreateMapIndex
				p.nextStep = stepFinalIndexName

			case "drop_auto_index":
				o.oType = typeDropAutoIndex
				p.nextStep = stepFinalIndexName

			case "drop_map_index":
				o.oType = typeDropMapIndex
				p.nextStep = stepFinalIndexName

			default:
				dslErr = syntaxError(keyword, p.remaining(), "unknown keyword")
				return
			}
		case stepQueryIndexName:
			o.indexName, err = p.current()
			if err != nil {
				dslErr = unexpectedEndOfInputError(p.raw, "index name")
				return
			}
			p.nextStep = stepFinalAutoSelector

		case stepQueryKeyIndexName:
			o.indexName, err = p.current()
			if err != nil {
				dslErr = unexpectedEndOfInputError(p.raw, "index name")
				return
			}
			p.nextStep = stepFinalMapSelector

		case stepInsertIndexName:
			o.indexName, err = p.current()
			if err != nil {
				dslErr = unexpectedEndOfInputError(p.raw, "index name")
				return
			}
			p.nextStep = stepFinalPayload

		case stepUpdateInsertKeyIndexName:
			o.indexName, err = p.current()
			if err != nil {
				dslErr = unexpectedEndOfInputError(p.raw, "index name")
				return
			}
			p.nextStep = stepUpdateInsertKeyMapSelector

		case stepUpdateIndexName:
			o.indexName, err = p.current()
			if err != nil {
				dslErr = unexpectedEndOfInputError(p.raw, "index name")
				return
			}
			p.nextStep = stepUpdateAutoSelector

		case stepDeleteIndexName:
			o.indexName, err = p.current()
			if err != nil {
				dslErr = unexpectedEndOfInputError(p.raw, "index name")
				return
			}
			p.nextStep = stepFinalAutoSelector

		case stepDeleteKeyIndexName:
			o.indexName, err = p.current()
			if err != nil {
				dslErr = unexpectedEndOfInputError(p.raw, "index name")
				return
			}
			p.nextStep = stepFinalMapSelector

		case stepListIndexName:
			o.indexName, err = p.current()
			if err != nil {
				dslErr = unexpectedEndOfInputError(p.raw, "index name")
				return
			}
			p.nextStep = stepListOptionalLimitOrDirection

		case stepListKeyIndexName:
			o.indexName, err = p.current()
			if err != nil {
				dslErr = unexpectedEndOfInputError(p.raw, "index name")
				return
			}
			p.nextStep = stepListOptionalLimitOrDirection

		case stepListOptionalLimitOrDirection:
			// optional token, error can be ignored
			token, _ := p.current()
			// if token is a direction, set the direction and treat as final token
			if desc, isDirection := parseDirection(token); isDirection {
				o.listDesc = desc
				return
			}

			// else treat as limit
			o.limit, err = strconv.Atoi(token)
			if err != nil {
				// if token is not empty, limit was invalid
				if token != "" {
					dslErr = parsingError(token, p.remaining(), "invalid limit", err)
					return
				}
			}
			p.nextStep = stepListOptionalOffsetOrDirection

		case stepFinalIndexName:
			o.indexName, err = p.current()
			if err != nil {
				dslErr = unexpectedEndOfInputError(p.raw, "index name")
				return
			}
			return

		case stepFinalAutoSelector:
			token, err := p.current()
			if err != nil {
				dslErr = unexpectedEndOfInputError(p.raw, "auto index selector")
				return
			}
			o.autoSel, err = ParseAutoSelector(token)
			if err != nil {
				dslErr = parsingError(token, p.remaining(), "invalid selector", err)
			}
			return

		case stepFinalMapSelector:
			token, err := p.current()
			if err != nil {
				dslErr = unexpectedEndOfInputError(p.raw, "map index selector")
				return
			}
			o.mapSel, err = ParseMapSelector(token)
			if err != nil {
				dslErr = parsingError(token, p.remaining(), "invalid selector", err)
				return
			}
			return

		case stepFinalPayload:
			payload := strings.Join(p.remaining(), " ")
			if payload == "" {
				dslErr = unexpectedEndOfInputError(p.raw, "insert payload")
			}
			o.payload = payload
			return

		case stepListOptionalOffsetOrDirection:
			// optional token, error can be ignored
			token, _ := p.current()
			// if token is a direction, treat as final token
			if desc, isDirection := parseDirection(token); isDirection {
				o.listDesc = desc
				return
			}

			o.offset, err = strconv.Atoi(token)
			if err != nil {
				// if token is not empty, offset was invalid
				if token != "" {
					dslErr = parsingError(token, p.remaining(), "invalid offset", err)
					return
				}
			}
			p.nextStep = stepFinalOptionalDirection

		case stepUpdateInsertKeyMapSelector:
			token, err := p.current()
			if err != nil {
				dslErr = unexpectedEndOfInputError(p.raw, "map index selector")
				return
			}
			o.mapSel, err = ParseMapSelector(token)
			if err != nil {
				dslErr = parsingError(token, p.remaining(), "invalid selector", err)
				return
			}
			p.nextStep = stepFinalPayload

		case stepUpdateAutoSelector:
			token, err := p.current()
			if err != nil {
				dslErr = unexpectedEndOfInputError(p.raw, "auto index selector")
				return
			}
			o.autoSel, err = ParseAutoSelector(token)
			if err != nil {
				dslErr = parsingError(token, p.remaining(), "invalid selector", err)
				return
			}
			p.nextStep = stepFinalPayload

		case stepFinalOptionalDirection:
			// optional token, error can be ignored
			token, _ := p.current()
			if desc, isDirection := parseDirection(token); isDirection {
				o.listDesc = desc
				// if token is not a valid direction and isn't empty, invalid syntax
			} else if token != "" {
				dslErr = syntaxError(token, p.remaining(), "invalid direction")
			}
			return

		default:
			dslErr = fmt.Errorf("internal error: unexpected state encountered while parsing query '%s'", p.raw)
			log.Errorf(err.Error())
			return
		}

		p.increment()
	}
}

// parse a token that may indicate a sort direction, defaulting to false
func parseDirection(token string) (desc bool, isDirection bool) {
	if token == "asc" {
		desc = false
		isDirection = true
		return
	} else if token == "desc" {
		desc = true
		isDirection = true
		return
	}

	return
}
