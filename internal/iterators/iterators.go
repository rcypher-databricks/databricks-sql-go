package iterators

import (
	"errors"
	"io"
)

type Iterator[T any] interface {
	HasNext() bool
	Close()
	Next() (T, error)
}

// Create an iterator that applies a transform function to each element
// of an input iterator
func NewTransformingIterator[I any, O any](input Iterator[I], transform func(I) (O, error)) Iterator[O] {
	return &transformingIterator[I, O]{
		input:     input,
		transform: transform,
	}
}

// An iterator that transforms the elements of an underlying input iterator
type transformingIterator[I any, O any] struct {
	// flag indicating that no further elements are available
	isFinished bool

	// an iterator providing the elements to transform
	input Iterator[I]

	// function to apply to elements of 'input'
	transform func(I) (O, error)
}

// Returns true if there are further elements that can be
// retrieved by calling Next()
func (si *transformingIterator[I, O]) HasNext() bool {
	hasNext := si.hasNext()
	if !hasNext {
		// If there are no more elements proactively close the iterator
		si.Close()
	}

	return hasNext
}

// Free up any resources associated with the iterator.
// Calling Close() more than once has no additional effect.
func (si *transformingIterator[I, O]) Close() {
	// Only want to free resources once
	if !si.isFinished {
		si.isFinished = true

		// Close the input iterator
		if si.input != nil {
			si.input.Close()
			si.input = nil
		}
	}
}

// Retrieve the next element of this iterator. If no more
// elements are available the zero value of the element type
// and io.EOF will be returned.
func (si *transformingIterator[I, O]) Next() (O, error) {
	if !si.HasNext() {
		// no more elements
		return errOut[O](si, io.EOF)
	}

	// If there is no transform function we are in an error state
	if si.transform == nil {
		return errOut[O](si, errors.New("transforming iterator requires transform function"))
	}

	// Get the next element of the input iterator
	nextInput, err := si.input.Next()
	if err != nil {
		return errOut[O](si, err)
	}

	// apply the transform function to the input element
	nextOutput, err := si.transform(nextInput)
	if err != nil {
		return errOut[O](si, err)
	}

	// If this is the last element proactively close the iterator
	if !si.input.HasNext() {
		si.Close()
	}

	return nextOutput, nil
}

// The logic to determine if the iterator has any more elements
func (si *transformingIterator[I, O]) hasNext() bool {
	inputHasNext := si.input != nil && si.input.HasNext()
	hasNext := !si.isFinished && inputHasNext
	return hasNext
}

func NewStacked1ToNIterator[O any](input Iterator[Iterator[O]], initSubIterator Iterator[O]) Iterator[O] {
	return &stacked1ToNIterator[O]{
		input:       input,
		subIterator: initSubIterator,
	}
}

type stacked1ToNIterator[O any] struct {
	isFinished  bool
	input       Iterator[Iterator[O]]
	subIterator Iterator[O]
}

func (si *stacked1ToNIterator[O]) HasNext() bool {
	hasNext := si.hasNext()
	if !hasNext {
		si.Close()
	}

	return hasNext
}

func (si *stacked1ToNIterator[O]) Next() (O, error) {

	if !si.HasNext() {
		return errOut[O](si, io.EOF)
	}

	if si.subIterator == nil || !si.subIterator.HasNext() {
		if si.subIterator != nil {
			si.subIterator.Close()
		}
		sub, err := si.input.Next()
		if err != nil {
			return errOut[O](si, err)
		}
		si.subIterator = sub
	}

	nextOutput, err := si.subIterator.Next()
	if err != nil {
		return errOut[O](si, err)
	}

	if !si.subIterator.HasNext() && (si.input == nil || !si.input.HasNext()) {
		si.Close()
	}

	return nextOutput, nil
}

func (si *stacked1ToNIterator[O]) Close() {
	if !si.isFinished {
		si.isFinished = true

		if si.subIterator != nil {
			si.subIterator.Close()
			si.subIterator = nil
		}

		if si.input != nil {
			si.input.Close()
			si.input = nil
		}
	}
}

func (si *stacked1ToNIterator[O]) hasNext() bool {
	subHasNext := si.subIterator != nil && si.subIterator.HasNext()
	inputHasNext := si.input != nil && si.input.HasNext()
	hasNext := !si.isFinished && (subHasNext || inputHasNext)

	return hasNext
}

// type sliceIterator[O any] struct {
// 	slice      []O
// 	index      int
// 	isFinished bool
// }

// func (si *sliceIterator[O]) HasNext() bool {
// 	return !si.isFinished && si.index < len(si.slice)
// }

// func (si *sliceIterator[O]) Next() (O, error) {
// 	if !si.HasNext() {
// 		return errOut[O](si, io.EOF)
// 	}
// 	e := si.slice[si.index]
// 	si.index += 1

// 	if si.index >= len(si.slice) {
// 		si.Close()
// 	}

// 	return e, nil
// }

// func (si *sliceIterator[O]) Close() {
// 	if !si.isFinished {
// 		si.isFinished = true
// 		si.slice = nil
// 	}

// }

func errOut[O any](c interface{ Close() }, err error) (O, error) {
	c.Close()
	var o O
	return o, err
}
