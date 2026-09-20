package spark

import "reflect"

func combineMapOutput(items []any, dep *ShuffleDep) []any {
	if dep == nil || !dep.MapSideCombine() || dep.Aggregator() == nil || len(items) == 0 {
		return items
	}
	agg := dep.Aggregator()
	type slot struct {
		sample any
		comb   any
	}
	slots := make(map[any]*slot)
	for _, item := range items {
		key := dep.ExtractKey(item)
		if key == nil {
			continue
		}
		val := pairValue(item)
		s, ok := slots[key]
		if !ok {
			slots[key] = &slot{sample: item, comb: agg.CreateCombiner(val)}
			continue
		}
		s.comb = agg.MergeValue(s.comb, val)
	}
	out := make([]any, 0, len(slots))
	for _, s := range slots {
		out = append(out, replacePairValue(s.sample, s.comb))
	}
	return out
}

func pairValue(item any) any {
	rv := reflect.ValueOf(item)
	if rv.Kind() == reflect.Struct {
		f := rv.FieldByName("Value")
		if f.IsValid() {
			return f.Interface()
		}
	}
	return item
}

func replacePairValue(sample, val any) any {
	rv := reflect.ValueOf(sample)
	if rv.Kind() != reflect.Struct {
		return val
	}
	out := reflect.New(rv.Type()).Elem()
	out.Set(rv)
	f := out.FieldByName("Value")
	if f.IsValid() && f.CanSet() {
		fv := reflect.ValueOf(val)
		if fv.IsValid() && fv.Type().AssignableTo(f.Type()) {
			f.Set(fv)
		}
	}
	return out.Interface()
}
