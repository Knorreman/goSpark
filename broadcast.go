package spark

import (
	"bytes"
	"encoding/gob"
	"fmt"
)

const maxBroadcastBytes = 8 << 20

// Broadcast is a named, gob-encoded value carried in JobSpec. Workers
// reconstruct it when the registered job factory runs; it is not a shuffled RDD.
type Broadcast struct {
	Name string `json:"name"`
	Gob  []byte `json:"gob"`
}

// BroadcastVar is a read-only-by-convention handle for a value used by RDD
// callbacks. A local handle does not automatically ship its value to workers;
// distributed jobs must also place the value in JobSpec with AddBroadcast.
type BroadcastVar[T any] struct{ value T }

// NewBroadcast creates a handle scoped to a local context or reconstructed
// worker job. Use ReadBroadcastVar when that worker value comes from JobSpec.
func NewBroadcast[T any](_ *Context, value T) BroadcastVar[T] {
	return BroadcastVar[T]{value: value}
}

func (b BroadcastVar[T]) Value() T { return b.value }

// ReadBroadcastVar rebuilds a broadcast handle inside a registered job factory.
func ReadBroadcastVar[T any](ctx *Context, spec JobSpec, name string) (BroadcastVar[T], error) {
	value, err := ReadBroadcast[T](spec, name)
	if err != nil {
		return BroadcastVar[T]{}, err
	}
	return NewBroadcast(ctx, value), nil
}

// AddBroadcast encodes value onto spec. The same spec must be submitted to
// every worker. Values are read-only after encoding.
func AddBroadcast(spec *JobSpec, name string, value any) error {
	if spec == nil {
		return fmt.Errorf("nil job spec")
	}
	if name == "" || len(name) > 128 {
		return fmt.Errorf("broadcast name must be 1-128 characters")
	}
	for _, existing := range spec.Broadcasts {
		if existing.Name == name {
			return fmt.Errorf("duplicate broadcast %q", name)
		}
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(value); err != nil {
		return fmt.Errorf("encode broadcast %q: %w", name, err)
	}
	if buf.Len() > maxBroadcastBytes {
		return fmt.Errorf("broadcast %q is %d bytes; limit is %d", name, buf.Len(), maxBroadcastBytes)
	}
	var total int
	for _, existing := range spec.Broadcasts {
		total += len(existing.Gob)
	}
	if total+buf.Len() > maxBroadcastBytes {
		return fmt.Errorf("broadcasts exceed %d byte limit", maxBroadcastBytes)
	}
	spec.Broadcasts = append(spec.Broadcasts, Broadcast{Name: name, Gob: buf.Bytes()})
	return nil
}

// ReadBroadcast decodes a value previously stored with AddBroadcast.
func ReadBroadcast[T any](spec JobSpec, name string) (T, error) {
	var zero T
	for _, b := range spec.Broadcasts {
		if b.Name != name {
			continue
		}
		var value T
		if err := gob.NewDecoder(bytes.NewReader(b.Gob)).Decode(&value); err != nil {
			return zero, fmt.Errorf("decode broadcast %q: %w", name, err)
		}
		return value, nil
	}
	return zero, fmt.Errorf("broadcast %q not found", name)
}
