package nativebootstrap

import (
	"bytes"
	"encoding/json"
	"io"
	"reflect"
	"strings"
)

// DecodeObject reads a closed, flat protocol object. Duplicate keys are rejected
// before struct decoding so there is only one interpretation of signed fields.
func DecodeObject(data []byte, dst any) error {
	typ := reflect.TypeOf(dst)
	if typ == nil || typ.Kind() != reflect.Pointer || typ.Elem().Kind() != reflect.Struct {
		return ErrInvalid
	}
	allowed := map[string]bool{}
	typ = typ.Elem()
	for i := 0; i < typ.NumField(); i++ {
		name := strings.Split(typ.Field(i).Tag.Get("json"), ",")[0]
		if name != "" && name != "-" {
			allowed[name] = true
		}
	}
	d := json.NewDecoder(bytes.NewReader(data))
	token, err := d.Token()
	if err != nil || token != json.Delim('{') {
		return ErrInvalid
	}
	seen := map[string]bool{}
	for d.More() {
		token, err = d.Token()
		if err != nil {
			return ErrInvalid
		}
		key, ok := token.(string)
		if !ok || !allowed[key] || seen[key] {
			return ErrInvalid
		}
		seen[key] = true
		var value json.RawMessage
		if d.Decode(&value) != nil {
			return ErrInvalid
		}
	}
	token, err = d.Token()
	if err != nil || token != json.Delim('}') {
		return ErrInvalid
	}
	if _, err = d.Token(); err != io.EOF {
		return ErrInvalid
	}
	d = json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if d.Decode(dst) != nil {
		return ErrInvalid
	}
	return nil
}
