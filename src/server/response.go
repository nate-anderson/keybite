package server

import (
	"keybite/store"
	"keybite/util/log"
)

// NullableString is a string that can marshall to JSON `null` if no valid value passed
type NullableString struct {
	value string
	valid bool
}

// ResultSet is a set of query results used when serving the JSON API
type ResultSet map[string]NullableString

// MarshalJSON makes the string fulfill the Marshaler interface
func (s NullableString) MarshalJSON() ([]byte, error) {
	if !s.valid {
		return []byte(`null`), nil
	}
	return []byte(`"` + s.value + `"`), nil
}

func toNullableString(str string) NullableString {
	return NullableString{
		valid: true,
		value: str,
	}
}

func resultToNullableString(r store.Result) NullableString {
	var valid bool
	bytes, err := r.MarshalJSON()
	if err != nil {
		log.Infof("error converting response to nullable JSON string: %s", err.Error())
		valid = false
	}
	return NullableString{
		valid: valid,
		value: string(bytes),
	}
}

// HasKeys indicates if the map contain all of the provided keys
func (r ResultSet) HasKeys(keys []string) bool {
	for _, key := range keys {
		if _, ok := r[key]; !ok {
			return false
		}
	}
	return true
}

// HasKey indicates if the map contain the provided key
func (r ResultSet) HasKey(key string) bool {
	if _, ok := r[key]; !ok {
		return false
	}

	return true
}

// GetValueList returns a collection of map values from a collection of keys
func (r ResultSet) GetValueList(keys []string) []interface{} {
	res := []string{}
	for _, key := range keys {
		res = append(res, r[key].value)
	}
	return strSliceToInterfaceSlice(res)
}

func strSliceToInterfaceSlice(strSlice []string) []interface{} {
	new := make([]interface{}, len(strSlice))
	for i, v := range strSlice {
		new[i] = v
	}
	return new
}
