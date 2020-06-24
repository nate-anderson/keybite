package server

// NullableString is a string that can marshall to JSON `null` if no valid value passed
type NullableString struct {
	value string
	valid bool
}

// ResultSet is a set of query results used when serving the JSON API
type ResultSet map[string]NullableString

// MarshalText makes the string fulfill the Marshaler interface
func (s NullableString) MarshalText() ([]byte, error) {
	if !s.valid {
		return []byte("null"), nil
	}
	return []byte(s.value), nil
}

func toNullableString(str string) NullableString {
	return NullableString{
		valid: true,
		value: str,
	}
}

// does the map contain all of the provided keys
func resultSetHasKeys(m ResultSet, keys []string) bool {
	for _, key := range keys {
		if _, ok := m[key]; !ok {
			return false
		}
	}
	return true
}

// get a collection of map values from a collection of keys
func getResultSetValues(m ResultSet, keys []string) []interface{} {
	res := []string{}
	for _, key := range keys {
		res = append(res, m[key].value)
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
