package raft

type Optional[T any] struct {
	value T
	set   bool
}

func Some[T any](v T) Optional[T] {
	return Optional[T]{value: v, set: true}
}

func None[T any]() Optional[T] {
	return Optional[T]{set: false}
}

func (opt *Optional[T]) Set(value T) {
	opt.value = value
	opt.set = true
}

func (opt Optional[T]) HasValue() bool {
	return opt.set
}

func (opt Optional[T]) Value() T {
	if !opt.set {
		panic("Tried to get an unset optional.")
	}
	return opt.value
}

func (opt Optional[T]) ValueOr(defaultValue T) T {
	if !opt.set {
		return defaultValue
	}
	return opt.value
}
