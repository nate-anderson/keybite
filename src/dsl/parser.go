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
	stepFinalOptionalOffset
	stepQueryIndexName
	stepInsertIndexName
	stepQueryKeyIndexName
	stepUpdateInsertKeyMapSelector
	stepUpdateAutoSelector
	stepListOptionalLimit
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

func (p parser) current() string {
	if len(p.tokens) > p.i {
		return p.tokens[p.i]
	}
	return ""
}

func (p *parser) increment() {
	p.i++
}

func (p parser) remaining() []string {
	return p.tokens[p.i:]
}

// Parse the provided query
func (p parser) Parse() (o Operation, err error) {
	for p.i < len(p.tokens) {
		switch p.nextStep {
		case stepInitial:
			keyword := p.current()
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
				err = fmt.Errorf("unknown keyword '%s'", keyword)
				return
			}
		case stepQueryIndexName:
			o.indexName = p.current()
			p.nextStep = stepFinalAutoSelector

		case stepQueryKeyIndexName:
			o.indexName = p.current()
			p.nextStep = stepFinalMapSelector

		case stepInsertIndexName:
			o.indexName = p.current()
			p.nextStep = stepFinalPayload

		case stepUpdateInsertKeyIndexName:
			o.indexName = p.current()
			p.nextStep = stepUpdateInsertKeyMapSelector

		case stepUpdateIndexName:
			o.indexName = p.current()
			p.nextStep = stepUpdateAutoSelector

		case stepDeleteIndexName:
			o.indexName = p.current()
			p.nextStep = stepFinalAutoSelector

		case stepDeleteKeyIndexName:
			o.indexName = p.current()
			p.nextStep = stepFinalMapSelector

		case stepListIndexName:
			o.indexName = p.current()
			p.nextStep = stepListOptionalLimit

		case stepListKeyIndexName:
			o.indexName = p.current()
			p.nextStep = stepListOptionalLimit

		case stepFinalIndexName:
			o.indexName = p.current()
			return

		case stepFinalAutoSelector:
			o.autoSel, err = ParseAutoSelector(p.current())
			return

		case stepFinalMapSelector:
			o.mapSel = ParseMapSelector(p.current())
			return

		case stepFinalPayload:
			o.payload = strings.Join(p.remaining(), " ")
			return

		case stepFinalOptionalOffset:
			token := p.current()
			o.offset, err = strconv.Atoi(token)
			if err != nil {
				// if token is not empty, offset was invalid
				if token != "" {
					err = fmt.Errorf("error parsing offset '%s': %s", token, err.Error())
				}
			}
			return

		case stepUpdateInsertKeyMapSelector:
			o.mapSel = ParseMapSelector(p.current())
			p.nextStep = stepFinalPayload

		case stepUpdateAutoSelector:
			o.autoSel, err = ParseAutoSelector(p.current())
			if err != nil {
				err = fmt.Errorf("error parsing auto selector '%s'", p.current())
				return
			}
			p.nextStep = stepFinalPayload

		case stepListOptionalLimit:
			token := p.current()
			o.limit, err = strconv.Atoi(token)
			if err != nil {
				// if token is not empty, limit was invalid
				if token != "" {
					err = fmt.Errorf("error parsing limit '%s': %s", token, err.Error())
				}
			}
			p.nextStep = stepFinalOptionalOffset

		default:
			err = fmt.Errorf("internal error: unexpected state encountered while parsing query '%s'", p.raw)
			log.Errorf(err.Error())
			return
		}

		p.increment()
	}

	return
}
