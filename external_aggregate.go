package spark

import (
	"fmt"
	"io"
	"math"
	"reflect"
	"strconv"
	"strings"
)

// Equality token for value keys. Pointer/channel keys are process-local and
// cannot safely participate in distributed grouping; reject them explicitly.
func keyToken(key any) string {
	var b strings.Builder
	var visit func(reflect.Value)
	visit = func(v reflect.Value) {
		if !v.IsValid() {
			b.WriteString("nil;")
			return
		}
		b.WriteString(v.Type().String())
		b.WriteString(v.Type().PkgPath())
		b.WriteByte(':')
		switch v.Kind() {
		case reflect.Interface:
			if v.IsNil() {
				b.WriteString("nil")
			} else {
				visit(v.Elem())
			}
		case reflect.String:
			b.WriteString(strconv.Quote(v.String()))
		case reflect.Bool:
			b.WriteString(strconv.FormatBool(v.Bool()))
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			b.WriteString(strconv.FormatInt(v.Int(), 10))
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
			b.WriteString(strconv.FormatUint(v.Uint(), 10))
		case reflect.Float32, reflect.Float64:
			f := v.Float()
			if math.IsNaN(f) {
				must(fmt.Errorf("NaN shuffle keys unsupported"))
			}
			if f == 0 {
				f = 0
			}
			b.WriteString(strconv.FormatFloat(f, 'g', -1, 64))
		case reflect.Struct:
			for i := 0; i < v.NumField(); i++ {
				visit(v.Field(i))
			}
		case reflect.Array:
			for i := 0; i < v.Len(); i++ {
				visit(v.Index(i))
			}
		default:
			must(fmt.Errorf("unsupported distributed key type %s", v.Type()))
		}
		b.WriteByte(';')
	}
	visit(reflect.ValueOf(key))
	return b.String()
}

func sortedPairs[K comparable, V any](ctx *Context, sid, rid, maps int) recordStream {
	s, _ := externalSort(ctx, shuffleStream(ctx, sid, rid, maps), func(a, b any) bool { return keyToken(a.(Pair[K, V]).Key) < keyToken(b.(Pair[K, V]).Key) })
	return s
}

func aggregatePairs[K comparable, V, C any](ctx *Context, sid, rid, maps int, create func(V) C, merge func(C, V) C, limit int) Iterator[Pair[K, C]] {
	s := sortedPairs[K, V](ctx, sid, rid, maps)
	ctx.onClose(func() { s.Close() })
	var pending *Pair[K, V]
	return func() (Pair[K, C], bool) {
		ctx.checkCanceled()
		var first Pair[K, V]
		if pending != nil {
			first = *pending
			pending = nil
		} else {
			v, err := s.Next()
			if err == io.EOF {
				return Pair[K, C]{}, false
			}
			must(err)
			first = v.(Pair[K, V])
		}
		acc := create(first.Value)
		check := func() {
			p, err := DefaultCodec().Encode(acc)
			must(err)
			if len(p) > limit {
				must(fmt.Errorf("group/combiner for key %v exceeds %d encoded bytes", first.Key, limit))
			}
		}
		// Register concrete result types for disk persistence and size checks.
		registerRecord(acc)
		check()
		for {
			ctx.checkCanceled()
			v, err := s.Next()
			if err == io.EOF {
				break
			}
			must(err)
			p := v.(Pair[K, V])
			if p.Key != first.Key {
				pending = &p
				break
			}
			acc = merge(acc, p.Value)
			check()
		}
		return NewPair(first.Key, acc), true
	}
}

func boundedGroups[K comparable, V any](ctx *Context, sid, rid, maps int) Iterator[Pair[K, []V]] {
	s := sortedPairs[K, V](ctx, sid, rid, maps)
	ctx.onClose(func() { s.Close() })
	var pending *Pair[K, V]
	return func() (Pair[K, []V], bool) {
		var first Pair[K, V]
		if pending != nil {
			first = *pending
			pending = nil
		} else {
			v, err := s.Next()
			if err == io.EOF {
				return Pair[K, []V]{}, false
			}
			must(err)
			first = v.(Pair[K, V])
		}
		var values []V
		size := 0
		add := func(v V) {
			registerRecord(v)
			p, err := DefaultCodec().Encode(v)
			must(err)
			size += len(p) + 32
			if size > ctx.groupBytes() {
				must(fmt.Errorf("GroupByKey key %v exceeds MaxGroupBytes=%d", first.Key, ctx.groupBytes()))
			}
			values = append(values, v)
		}
		add(first.Value)
		for {
			ctx.checkCanceled()
			v, err := s.Next()
			if err == io.EOF {
				break
			}
			must(err)
			p := v.(Pair[K, V])
			if p.Key != first.Key {
				pending = &p
				break
			}
			add(p.Value)
		}
		return NewPair(first.Key, values), true
	}
}
