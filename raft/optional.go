package raft

type Optional[T any] struct {
	Val     T
	Present bool
}

func Some[T any](v T) Optional[T] {
	return Optional[T]{Val: v, Present: true}
}

func None[T any]() Optional[T] {
	return Optional[T]{Present: false}
}

func (opt *Optional[T]) Set(value T) {
	opt.Val = value
	opt.Present = true
}

func (opt Optional[T]) HasValue() bool {
	return opt.Present
}

func (opt Optional[T]) Value() T {
	if !opt.Present {
		panic("Tried to get an unset optional.")
	}
	return opt.Val
}

func (opt Optional[T]) ValueOr(defaultValue T) T {
	if !opt.Present {
		return defaultValue
	}
	return opt.Val
}
