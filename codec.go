package spark

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"sync"
)

func init() {
	gob.Register(Pair[string, int]{})
	gob.Register(Pair[string, []int]{})
	gob.Register(Pair[int, int]{})
	gob.Register(Pair[int, string]{})
	gob.Register(Pair[string, string]{})
	gob.Register(Pair[int, Pair[string, int]]{})
	gob.Register(int(0))
	gob.Register("")
}

type RecordCodec interface {
	Name() string
	Encode(v any) ([]byte, error)
	Decode(b []byte) (any, error)
}

type gobCodec struct{}

func (gobCodec) Name() string { return "gob" }

func (gobCodec) Encode(v any) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(&v); err != nil {
		return nil, fmt.Errorf("gob encode: %w", err)
	}
	return buf.Bytes(), nil
}

func (gobCodec) Decode(b []byte) (any, error) {
	var v any
	if err := gob.NewDecoder(bytes.NewReader(b)).Decode(&v); err != nil {
		return nil, fmt.Errorf("gob decode: %w", err)
	}
	return v, nil
}

var (
	codecMu      sync.RWMutex
	defaultCodec RecordCodec = gobCodec{}
)

func DefaultCodec() RecordCodec {
	codecMu.RLock()
	defer codecMu.RUnlock()
	return defaultCodec
}
