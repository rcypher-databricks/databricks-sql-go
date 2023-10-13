package iterators

import "io"

type Iterator[T any] interface {
	HasNext() bool
	Close()
	Next() (T, error)
}

func NewTransformingIterator[I any, O any](input Iterator[I], transform func(I) (O, error)) Iterator[O] {
	return &transformingIterator[I, O]{
		Input:     input,
		Transform: transform,
	}
}

type transformingIterator[I any, O any] struct {
	isFinished bool
	Input      Iterator[I]
	Transform  func(I) (O, error)
}

func (si *transformingIterator[I, O]) HasNext() bool {
	return !si.isFinished
}

func (si *transformingIterator[I, O]) Close() {
	if !si.isFinished {
		si.isFinished = true

		if si.Input != nil {
			si.Input.Close()
		}
	}
}

func (si *transformingIterator[I, O]) Next() (O, error) {
	if !si.HasNext() {
		return errOut[O](si, io.EOF)
	}

	nextInput, err := si.Input.Next()
	if err != nil {
		return errOut[O](si, err)
	}

	nextOutput, err := si.Transform(nextInput)
	if err != nil {
		return errOut[O](si, err)
	}

	if !si.Input.HasNext() {
		si.Close()
	}

	return nextOutput, nil
}

func NewStacked1ToNIterator[O any](input Iterator[Iterator[O]], sub Iterator[O]) Iterator[O] {
	return &stacked1ToNIterator[O]{
		Input:       input,
		SubIterator: sub,
	}
}

type stacked1ToNIterator[O any] struct {
	isFinished  bool
	Input       Iterator[Iterator[O]]
	SubIterator Iterator[O]
}

func (si *stacked1ToNIterator[O]) HasNext() bool {
	return !si.isFinished
}

func (si *stacked1ToNIterator[O]) Next() (O, error) {

	if !si.HasNext() {
		return errOut[O](si, io.EOF)
	}

	if si.SubIterator == nil || !si.SubIterator.HasNext() {
		if si.SubIterator != nil {
			si.SubIterator.Close()
		}
		sub, err := si.Input.Next()
		if err != nil {
			return errOut[O](si, err)
		}
		si.SubIterator = sub
	}

	nextOutput, err := si.SubIterator.Next()
	if err != nil {
		return errOut[O](si, err)
	}

	if !si.SubIterator.HasNext() && !si.Input.HasNext() {
		si.Close()
	}

	return nextOutput, nil
}

func (si *stacked1ToNIterator[O]) Close() {
	if !si.isFinished {
		si.isFinished = true

		if si.SubIterator != nil {
			si.SubIterator.Close()
			si.SubIterator = nil
		}

		if si.Input != nil {
			si.Input.Close()
			si.Input = nil
		}
	}
}

func errOut[O any](c interface{ Close() }, err error) (O, error) {
	c.Close()
	var o O
	return o, err
}
