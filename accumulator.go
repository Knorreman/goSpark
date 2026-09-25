package spark

import (
	"fmt"
	"sync"
)

type AccumulatorDef struct {
	Name string `json:"name"`
	Kind string `json:"kind"`
}

type accumulatorValue struct {
	Kind    string  `json:"kind"`
	Int64   int64   `json:"int64,omitempty"`
	Float64 float64 `json:"float64,omitempty"`
}

type accumulatorSet struct {
	mu       sync.Mutex
	values   map[string]accumulatorValue
	task     bool
	declared bool
}

func (c *Context) accumulator(name, kind string) error {
	if name == "" || len(name) > 128 {
		return fmt.Errorf("accumulator name must be 1-128 characters")
	}
	c.accumulators.mu.Lock()
	defer c.accumulators.mu.Unlock()
	if c.accumulators.values == nil {
		c.accumulators.values = make(map[string]accumulatorValue)
	}
	existing, ok := c.accumulators.values[name]
	if c.accumulators.declared && (!ok || existing.Kind != kind) {
		return fmt.Errorf("accumulator %q is not declared as %s", name, kind)
	}
	if ok && existing.Kind != kind {
		return fmt.Errorf("accumulator %q has type %s, not %s", name, existing.Kind, kind)
	}
	if !ok {
		c.accumulators.values[name] = accumulatorValue{Kind: kind}
	}
	return nil
}

type Int64Accumulator struct {
	ctx  *Context
	name string
}

func NewInt64Accumulator(ctx *Context, name string) (*Int64Accumulator, error) {
	if ctx == nil {
		return nil, fmt.Errorf("nil accumulator context")
	}
	if err := ctx.accumulator(name, "int64"); err != nil {
		return nil, err
	}
	return &Int64Accumulator{ctx: ctx, name: name}, nil
}

func (a *Int64Accumulator) Add(delta int64) {
	s := &a.ctx.accumulators
	s.mu.Lock()
	v := s.values[a.name]
	v.Int64 += delta
	s.values[a.name] = v
	s.mu.Unlock()
}

func (a *Int64Accumulator) Value() (int64, error) {
	s := &a.ctx.accumulators
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.task {
		return 0, fmt.Errorf("task cannot read accumulator total")
	}
	return s.values[a.name].Int64, nil
}

type Float64Accumulator struct {
	ctx  *Context
	name string
}

func NewFloat64Accumulator(ctx *Context, name string) (*Float64Accumulator, error) {
	if ctx == nil {
		return nil, fmt.Errorf("nil accumulator context")
	}
	if err := ctx.accumulator(name, "float64"); err != nil {
		return nil, err
	}
	return &Float64Accumulator{ctx: ctx, name: name}, nil
}

func (a *Float64Accumulator) Add(delta float64) {
	s := &a.ctx.accumulators
	s.mu.Lock()
	v := s.values[a.name]
	v.Float64 += delta
	s.values[a.name] = v
	s.mu.Unlock()
}

func (a *Float64Accumulator) Value() (float64, error) {
	s := &a.ctx.accumulators
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.task {
		return 0, fmt.Errorf("task cannot read accumulator total")
	}
	return s.values[a.name].Float64, nil
}

// BindAccumulators attaches driver-owned accumulators to a distributed job.
// Call it after creating the accumulators and before submitting the spec.
func BindAccumulators(spec *JobSpec, ctx *Context) error {
	if spec == nil || ctx == nil {
		return fmt.Errorf("nil job spec or accumulator context")
	}
	ctx.accumulators.mu.Lock()
	defer ctx.accumulators.mu.Unlock()
	if ctx.accumulators.task {
		return fmt.Errorf("cannot bind task accumulators")
	}
	spec.Accumulators = nil
	for name, value := range ctx.accumulators.values {
		spec.Accumulators = append(spec.Accumulators, AccumulatorDef{Name: name, Kind: value.Kind})
	}
	spec.accumulatorContext = ctx
	return nil
}

func (c *Context) prepareAccumulators(spec JobSpec) {
	c.accumulators.task = true
	c.accumulators.declared = true
	c.accumulators.values = make(map[string]accumulatorValue)
	for _, def := range spec.Accumulators {
		c.accumulators.values[def.Name] = accumulatorValue{Kind: def.Kind}
	}
}

func (c *Context) accumulatorUpdates() map[string]accumulatorValue {
	c.accumulators.mu.Lock()
	defer c.accumulators.mu.Unlock()
	updates := make(map[string]accumulatorValue)
	for name, value := range c.accumulators.values {
		updates[name] = value
	}
	return updates
}

func mergeAccumulators(spec JobSpec, accepted map[taskKey]ExecResult) error {
	if spec.accumulatorContext == nil {
		return nil
	}
	s := &spec.accumulatorContext.accumulators
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, result := range accepted {
		for name, value := range result.Accumulators {
			registered, ok := s.values[name]
			if !ok || registered.Kind != value.Kind {
				return fmt.Errorf("unknown or mismatched accumulator %q", name)
			}
		}
	}
	for _, result := range accepted {
		for name, value := range result.Accumulators {
			registered := s.values[name]
			registered.Int64 += value.Int64
			registered.Float64 += value.Float64
			s.values[name] = registered
		}
	}
	return nil
}
