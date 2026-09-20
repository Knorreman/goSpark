package spark

type Iterator[T any] func() (T, bool)

func EmptyIterator[T any]() Iterator[T] {
	return func() (T, bool) {
		var zero T
		return zero, false
	}
}

func SliceIterator[T any](s []T) Iterator[T] {
	i := 0
	return func() (T, bool) {
		if i >= len(s) {
			var zero T
			return zero, false
		}
		v := s[i]
		i++
		return v, true
	}
}

func MapIterator[T any, U any](iter Iterator[T], fn func(T) U) Iterator[U] {
	return func() (U, bool) {
		v, ok := iter()
		if !ok {
			var zero U
			return zero, false
		}
		return fn(v), true
	}
}

func FlatMapIterator[T any, U any](iter Iterator[T], fn func(T) []U) Iterator[U] {
	var buf []U
	return func() (U, bool) {
		for len(buf) == 0 {
			v, ok := iter()
			if !ok {
				var zero U
				return zero, false
			}
			buf = fn(v)
		}
		u := buf[0]
		buf = buf[1:]
		return u, true
	}
}

func FilterIterator[T any](iter Iterator[T], fn func(T) bool) Iterator[T] {
	return func() (T, bool) {
		for {
			v, ok := iter()
			if !ok {
				var zero T
				return zero, false
			}
			if fn(v) {
				return v, true
			}
		}
	}
}

func CollectIterator[T any](iter Iterator[T]) []T {
	var result []T
	for {
		v, ok := iter()
		if !ok {
			return result
		}
		result = append(result, v)
	}
}

func CountIterator[T any](iter Iterator[T]) int64 {
	var count int64
	for {
		_, ok := iter()
		if !ok {
			return count
		}
		count++
	}
}

func ReduceIterator[T any](iter Iterator[T], fn func(T, T) T) (T, bool) {
	first, ok := iter()
	if !ok {
		var zero T
		return zero, false
	}
	acc := first
	for {
		v, ok := iter()
		if !ok {
			return acc, true
		}
		acc = fn(acc, v)
	}
}

func ForeachIterator[T any](iter Iterator[T], fn func(T)) {
	for {
		v, ok := iter()
		if !ok {
			return
		}
		fn(v)
	}
}

func TakeIterator[T any](iter Iterator[T], n int) []T {
	var result []T
	for i := 0; i < n; i++ {
		v, ok := iter()
		if !ok {
			return result
		}
		result = append(result, v)
	}
	return result
}

func ChainIterators[T any](iters []Iterator[T]) Iterator[T] {
	idx := 0
	return func() (T, bool) {
		for idx < len(iters) {
			v, ok := iters[idx]()
			if ok {
				return v, true
			}
			idx++
		}
		var zero T
		return zero, false
	}
}
