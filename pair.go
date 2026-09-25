package spark

import (
	"bytes"
	"encoding/gob"
	"reflect"
)

type Pair[K any, V any] struct {
	Key   K
	Value V
}

func NewPair[K any, V any](key K, value V) Pair[K, V] {
	return Pair[K, V]{Key: key, Value: value}
}

// gob omits pointer fields whose pointed-to value is zero. Carry their presence
// separately so an outer join's non-nil pointer to a zero value survives RPC.
func (p Pair[K, V]) GobEncode() ([]byte, error) {
	var buf bytes.Buffer
	fields := struct {
		Key          K
		Value        V
		KeyPresent   bool
		ValuePresent bool
	}{p.Key, p.Value, pointerPresent(p.Key), pointerPresent(p.Value)}
	err := gob.NewEncoder(&buf).Encode(fields)
	return buf.Bytes(), err
}

func (p *Pair[K, V]) GobDecode(data []byte) error {
	var fields struct {
		Key          K
		Value        V
		KeyPresent   bool
		ValuePresent bool
	}
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&fields); err != nil {
		return err
	}
	p.Key, p.Value = fields.Key, fields.Value
	restoreZeroPointer(&p.Key, fields.KeyPresent)
	restoreZeroPointer(&p.Value, fields.ValuePresent)
	return nil
}

func pointerPresent[T any](value T) bool {
	v := reflect.ValueOf(value)
	return v.IsValid() && v.Kind() == reflect.Pointer && !v.IsNil()
}

func restoreZeroPointer[T any](value *T, present bool) {
	if !present {
		return
	}
	v := reflect.ValueOf(value).Elem()
	if v.Kind() == reflect.Pointer && v.IsNil() {
		v.Set(reflect.New(v.Type().Elem()))
	}
}
