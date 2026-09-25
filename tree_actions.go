package spark

import (
	"fmt"
	"reflect"
	"sync"
)

// TreeAggregate folds each partition with its own copy of zero, then combines
// partition partials in partition order. seqOp and combOp should be associative;
// zero should be an identity for combOp. Mutable zero values are copied before
// they are passed to either callback.
func TreeAggregate[T, U any](rdd *RDD[T], zero U, seqOp func(U, T) U, combOp func(U, U) U) U {
	computeShuffleStages(rdd)
	parts := rdd.Partitions()
	results := make([]U, len(parts))
	var wg sync.WaitGroup
	for i, part := range parts {
		wg.Add(1)
		go func() {
			defer wg.Done()
			acc := copyZero(zero)
			it := rdd.Compute(part)
			for v, ok := it(); ok; v, ok = it() {
				acc = seqOp(acc, v)
			}
			results[i] = acc
		}()
	}
	wg.Wait()
	acc := copyZero(zero)
	for _, partial := range results {
		acc = combOp(acc, partial)
	}
	return acc
}

// TreeReduce reduces within each nonempty partition before combining partials.
// It returns false when every partition is empty.
func TreeReduce[T any](rdd *RDD[T], fn func(T, T) T) (T, bool) {
	return Reduce(rdd, fn)
}

type treeAction struct {
	partial func(IteratorAny) (any, bool)
	merge   func(any, any) any
	zero    func() any // nil for a reduction without an identity
}

var treeActions = map[string]treeAction{}

// RegisterTreeAggregate installs a named action in the driver and every worker.
// The job factory must return an RDD[T]; use name as JobSpec.Action in Schedule.
// Register it during application initialization, before scheduling tasks.
func RegisterTreeAggregate[T, U any](name string, zero U, seqOp func(U, T) U, combOp func(U, U) U) {
	registerRecord(zero)
	action := treeAction{
		zero: func() any { return copyZero(zero) },
		partial: func(it IteratorAny) (any, bool) {
			acc := copyZero(zero)
			for v, ok := it(); ok; v, ok = it() {
				acc = seqOp(acc, v.(T))
			}
			return acc, true
		},
		merge: func(a, b any) any { return combOp(a.(U), b.(U)) },
	}
	registerTreeAction(name, action)
}

// RegisterTreeReduce installs a named same-type reduction action for Schedule.
// Empty partitions send no partial; an entirely empty job returns no records.
func RegisterTreeReduce[T any](name string, fn func(T, T) T) {
	action := treeAction{
		partial: func(it IteratorAny) (any, bool) {
			first, ok := it()
			if !ok {
				return nil, false
			}
			acc := first.(T)
			for v, ok := it(); ok; v, ok = it() {
				acc = fn(acc, v.(T))
			}
			return acc, true
		},
		merge: func(a, b any) any { return fn(a.(T), b.(T)) },
	}
	registerTreeAction(name, action)
}

func registerTreeAction(name string, action treeAction) {
	RegisterAction(name, func(RDDAny, JobSpec) error { return nil })
	taskRegistryMu.Lock()
	treeActions[name] = action
	taskRegistryMu.Unlock()
}

func getTreeAction(name string) (treeAction, bool) {
	taskRegistryMu.Lock()
	action, ok := treeActions[name]
	taskRegistryMu.Unlock()
	return action, ok
}

func mergeTreeResults(action treeAction, results []ExecResult) ([]any, error) {
	var acc any
	found := false
	if action.zero != nil {
		acc, found = action.zero(), true
	}
	for _, result := range results {
		if len(result.Records) > 1 {
			return nil, fmt.Errorf("tree action returned multiple partials for partition %d", result.PartitionID)
		}
		if len(result.Records) == 0 {
			continue
		}
		if !found {
			acc, found = result.Records[0], true
		} else {
			acc = action.merge(acc, result.Records[0])
		}
	}
	if !found {
		return nil, nil
	}
	return []any{acc}, nil
}

// copyZero duplicates mutable containers so in-place callbacks cannot change
// the caller's zero or another partition's starting value.
func copyZero[T any](zero T) T {
	v := reflect.ValueOf(zero)
	if !v.IsValid() {
		return zero
	}
	return cloneValue(v, make(map[cloneKey]reflect.Value)).Interface().(T)
}

type cloneKey struct {
	typ reflect.Type
	ptr uintptr
}

func cloneValue(v reflect.Value, seen map[cloneKey]reflect.Value) reflect.Value {
	if !v.IsValid() {
		return v
	}
	switch v.Kind() {
	case reflect.Interface:
		if v.IsNil() {
			return v
		}
		out := reflect.New(v.Type()).Elem()
		out.Set(cloneValue(v.Elem(), seen))
		return out
	case reflect.Pointer, reflect.Map, reflect.Slice:
		if v.IsNil() {
			return v
		}
		var key cloneKey
		if v.Kind() != reflect.Map {
			key = cloneKey{v.Type(), v.Pointer()}
			if prior, ok := seen[key]; ok {
				return prior
			}
		}
		var out reflect.Value
		switch v.Kind() {
		case reflect.Pointer:
			out = reflect.New(v.Type().Elem())
			seen[key] = out
			out.Elem().Set(cloneValue(v.Elem(), seen))
		case reflect.Map:
			out = reflect.MakeMapWithSize(v.Type(), v.Len())
			iter := v.MapRange()
			for iter.Next() {
				out.SetMapIndex(iter.Key(), cloneValue(iter.Value(), seen))
			}
		case reflect.Slice:
			out = reflect.MakeSlice(v.Type(), v.Len(), v.Len())
			seen[key] = out
			for i := 0; i < v.Len(); i++ {
				out.Index(i).Set(cloneValue(v.Index(i), seen))
			}
		}
		return out
	case reflect.Array:
		out := reflect.New(v.Type()).Elem()
		for i := 0; i < v.Len(); i++ {
			out.Index(i).Set(cloneValue(v.Index(i), seen))
		}
		return out
	case reflect.Struct:
		out := reflect.New(v.Type()).Elem()
		out.Set(v)
		for i := 0; i < v.NumField(); i++ {
			if out.Field(i).CanSet() {
				out.Field(i).Set(cloneValue(v.Field(i), seen))
			}
		}
		return out
	default:
		return v
	}
}
