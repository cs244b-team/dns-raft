package main

type Optional[T any] struct {
	value T
	valid bool
}

func New[T any](v T) Optional[T] {
	return Optional[T]{value: v, valid: true}
}

func None[T any]() Optional[T] {
	return Optional[T]{valid: false}
}

func (opt *Optional[T]) Set(value T) {
	opt.value = value
	opt.valid = true
}

func (opt Optional[T]) HasValue() bool {
	return opt.valid
}

func (opt Optional[T]) Value() T {
	if !opt.valid {
		panic("Tried to get an unset optional.")
	}
	return opt.value
}
