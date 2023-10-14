package iterators

import (
	"errors"
	"io"
	"testing"

	"gotest.tools/v3/assert"
)

func TestTransformingIterator(t *testing.T) {
	t.Run("with no input", func(t *testing.T) {
		transform := func(i int) (bool, error) { return i < 3, nil }

		// calling HasNext first
		ti := NewTransformingIterator[int, bool](nil, transform).(*transformingIterator[int, bool])
		assert.Assert(t, !ti.HasNext())
		assert.Assert(t, ti.isFinished) // HasNext() should have closed the iterator
		b, err := ti.Next()
		assert.Assert(t, !b)
		assert.ErrorIs(t, err, io.EOF)
		assert.Assert(t, ti.isFinished)

		// calling Next first
		ti = NewTransformingIterator[int, bool](nil, transform).(*transformingIterator[int, bool])
		b, err = ti.Next()
		assert.Assert(t, !b)
		assert.ErrorIs(t, err, io.EOF)
		assert.Assert(t, ti.isFinished)
		assert.Assert(t, !ti.HasNext())

		// calling Close first
		ti = NewTransformingIterator[int, bool](nil, transform).(*transformingIterator[int, bool])
		ti.Close()
		assert.Assert(t, ti.isFinished)
		assert.Assert(t, !ti.HasNext())
		b, err = ti.Next()
		assert.Assert(t, !b)
		assert.ErrorIs(t, err, io.EOF)
	})

	t.Run("with no transform", func(t *testing.T) {
		ints := []int{2, 4, 3, 7, 1}
		si := &testSliceIterator[int]{slice: ints}
		// calling HasNext first
		ti := NewTransformingIterator[int, bool](si, nil).(*transformingIterator[int, bool])
		assert.Assert(t, ti.HasNext())
		assert.Assert(t, !ti.isFinished)

		b, err := ti.Next()
		assert.Assert(t, !b)
		assert.ErrorContains(t, err, "transforming iterator requires transform function")
		assert.Assert(t, ti.isFinished) // error should cause Close to be called

		si = &testSliceIterator[int]{slice: ints}
		ti = NewTransformingIterator[int, bool](si, nil).(*transformingIterator[int, bool])
		b, err = ti.Next()
		assert.Assert(t, !b)
		assert.ErrorContains(t, err, "transforming iterator requires transform function")
		assert.Assert(t, ti.isFinished)
		assert.Assert(t, !ti.HasNext())
	})

	t.Run("reading final item calls Close", func(t *testing.T) {
		ints := []int{2, 4, 3, 7, 1}
		si := &testSliceIterator[int]{slice: ints}
		transform := func(i int) (int, error) { return i, nil }
		ti := NewTransformingIterator[int, int](si, transform).(*transformingIterator[int, int])

		for range ints {
			assert.Assert(t, ti.HasNext())
			_, err := ti.Next()
			assert.NilError(t, err)
		}

		assert.Assert(t, ti.isFinished)
		assert.Assert(t, !ti.HasNext())
	})

	t.Run("Close closes input", func(t *testing.T) {
		ints := []int{2, 4, 3, 7, 1}
		si := &testSliceIterator[int]{slice: ints}
		transform := func(i int) (bool, error) { return i < 3, nil }

		ti := NewTransformingIterator[int, bool](si, transform).(*transformingIterator[int, bool])
		assert.Assert(t, ti.HasNext())

		ti.Close()
		assert.Assert(t, ti.isFinished)
		assert.Equal(t, 1, si.closeCount)
		assert.Assert(t, !ti.HasNext())
	})

	t.Run("calling Close more than once has no effect", func(t *testing.T) {
		ints := []int{2, 4, 3, 7, 1}
		si := &testSliceIterator[int]{slice: ints}
		transform := func(i int) (bool, error) { return i < 3, nil }

		ti := NewTransformingIterator[int, bool](si, transform).(*transformingIterator[int, bool])
		assert.Assert(t, ti.HasNext())

		ti.Close()
		assert.Assert(t, ti.isFinished)
		assert.Equal(t, 1, si.closeCount)
		assert.Assert(t, !ti.HasNext())

		// calling Close again should do nothing
		ti.Close()
		assert.Assert(t, !ti.HasNext())
		assert.Assert(t, ti.isFinished)
		assert.Equal(t, 1, si.closeCount)
	})

	t.Run("transforms inputs", func(t *testing.T) {
		ints := []int{2, 4, 3, 7, 1}
		si := &testSliceIterator[int]{slice: ints}
		transform := func(i int) (bool, error) { return i < 3, nil }
		ti := NewTransformingIterator[int, bool](si, transform).(*transformingIterator[int, bool])

		for i := range ints {
			assert.Assert(t, ti.HasNext())
			b, err := ti.Next()
			assert.Equal(t, ints[i] < 3, b)
			assert.NilError(t, err)
		}

		assert.Assert(t, ti.isFinished)
		assert.Assert(t, !ti.HasNext())
	})

	t.Run("handles transform failure", func(t *testing.T) {
		ints := []int{2, 4, 3, 7, 1}
		si := &testSliceIterator[int]{slice: ints}
		transform := func(int) (bool, error) { return false, errors.New("transform failed") }
		ti := NewTransformingIterator[int, bool](si, transform).(*transformingIterator[int, bool])
		assert.Assert(t, ti.HasNext())
		assert.Assert(t, !ti.isFinished)

		b, err := ti.Next()
		assert.Assert(t, !b)
		assert.ErrorContains(t, err, "transform failed")
		assert.Assert(t, ti.isFinished) // error condition should call close
		assert.Equal(t, 1, si.closeCount)

		// iterator has now been closed
		assert.Assert(t, !ti.HasNext())
		b, err = ti.Next()
		assert.Assert(t, !b)
		assert.ErrorIs(t, err, io.EOF)
	})

	t.Run("handles input error", func(t *testing.T) {
		ints := []int{2, 4, 3, 7, 1}
		si := &testSliceIterator[int]{slice: ints, fnNext: func() (int, error) { return 0, errors.New("input failed") }}
		transform := func(i int) (bool, error) { return i < 3, nil }
		ti := NewTransformingIterator[int, bool](si, transform).(*transformingIterator[int, bool])
		assert.Assert(t, ti.HasNext())
		assert.Assert(t, !ti.isFinished)

		b, err := ti.Next()
		assert.Assert(t, !b)
		assert.ErrorContains(t, err, "input failed")
		assert.Assert(t, ti.isFinished) // error condition should call close
		assert.Equal(t, 1, si.closeCount)

		// iterator has now been closed
		assert.Assert(t, !ti.HasNext())
		b, err = ti.Next()
		assert.Assert(t, !b)
		assert.ErrorIs(t, err, io.EOF)
	})
}

func TestStacked1ToNIterator(t *testing.T) {
	t.Run("no input", func(t *testing.T) {
		// call HasNext first
		it := NewStacked1ToNIterator[int](nil, nil).(*stacked1ToNIterator[int])
		assert.Assert(t, !it.HasNext())
		assert.Assert(t, it.isFinished) // HasNext should have called Close
		i, err := it.Next()
		assert.Equal(t, 0, i)
		assert.ErrorIs(t, err, io.EOF)

		// call Next first
		it = NewStacked1ToNIterator[int](nil, nil).(*stacked1ToNIterator[int])
		i, err = it.Next()
		assert.Equal(t, 0, i)
		assert.ErrorIs(t, err, io.EOF)
		assert.Assert(t, it.isFinished) // Next should have called Close

		assert.Assert(t, !it.HasNext())

		// call Close first
		it = NewStacked1ToNIterator[int](nil, nil).(*stacked1ToNIterator[int])
		it.Close()
		assert.Assert(t, it.isFinished)
		assert.Assert(t, !it.HasNext())
		i, err = it.Next()
		assert.Equal(t, 0, i)
		assert.ErrorIs(t, err, io.EOF)
	})

	t.Run("no input with initial sub-iterator", func(t *testing.T) {
		ints := []int{2, 4, 3, 7, 1}
		si := &testSliceIterator[int]{slice: ints}
		it := NewStacked1ToNIterator[int](nil, si).(*stacked1ToNIterator[int])

		for i := range ints {
			assert.Assert(t, it.HasNext())
			assert.Assert(t, !it.isFinished)

			e, err := it.Next()
			assert.NilError(t, err)
			assert.Equal(t, ints[i], e)
		}

		assert.Assert(t, it.isFinished) // reading last element should Close iterator
		assert.Assert(t, !it.HasNext())
		e, err := it.Next()
		assert.Equal(t, 0, e)
		assert.ErrorIs(t, err, io.EOF)
	})

	t.Run("input with no initial sub-iterator", func(t *testing.T) {
		ints := []int{2, 4, 3, 7, 1}
		si := &testSliceIterator[int]{slice: ints}
		var eCount int
		input := &testSliceIterator[Iterator[int]]{
			fnHasNext: func() bool { return eCount == 0 },
			fnNext: func() (Iterator[int], error) {
				if eCount > 0 {
					return nil, io.EOF
				}
				eCount += 1
				return si, nil
			},
		}

		it := NewStacked1ToNIterator[int](input, nil).(*stacked1ToNIterator[int])

		for i := range ints {
			assert.Assert(t, it.HasNext())
			assert.Assert(t, !it.isFinished)

			e, err := it.Next()
			assert.NilError(t, err)
			assert.Equal(t, ints[i], e)
		}

		assert.Assert(t, it.isFinished) // reading last element should Close iterator
		assert.Assert(t, !it.HasNext())
		e, err := it.Next()
		assert.Equal(t, 0, e)
		assert.ErrorIs(t, err, io.EOF)
	})

	t.Run("input with initial sub-iterator", func(t *testing.T) {
		ints := []int{2, 4, 3, 7, 1}
		si := &testSliceIterator[int]{slice: ints}
		ints2 := []int{8, 7, 12}
		si2 := &testSliceIterator[int]{slice: ints2}
		ints3 := []int{0, 1, 2}
		si3 := &testSliceIterator[int]{slice: ints3}

		var eCount int
		input := &testSliceIterator[Iterator[int]]{
			fnHasNext: func() bool { return eCount < 2 },
			fnNext: func() (Iterator[int], error) {
				if eCount >= 2 {
					return nil, io.EOF
				}
				eCount += 1
				if eCount == 1 {
					return si2, nil
				}
				return si3, nil
			},
		}

		it := NewStacked1ToNIterator[int](input, si).(*stacked1ToNIterator[int])

		// Should iterate over elements in init sub-iterator
		for i := range ints {
			assert.Assert(t, it.HasNext())
			assert.Assert(t, !it.isFinished)

			e, err := it.Next()
			assert.NilError(t, err)
			assert.Equal(t, ints[i], e)
		}

		// Followed by elements in sub-iterator returned by input
		for i := range ints2 {
			assert.Assert(t, it.HasNext())
			assert.Assert(t, !it.isFinished)

			e, err := it.Next()
			assert.NilError(t, err)
			assert.Equal(t, ints2[i], e)
		}

		for i := range ints3 {
			assert.Assert(t, it.HasNext())
			assert.Assert(t, !it.isFinished)

			e, err := it.Next()
			assert.NilError(t, err)
			assert.Equal(t, ints3[i], e)
		}

		assert.Assert(t, it.isFinished) // reading last element should Close iterator
		assert.Assert(t, !it.HasNext())
		e, err := it.Next()
		assert.Equal(t, 0, e)
		assert.ErrorIs(t, err, io.EOF)
	})

	t.Run("calling Close closes input and sub-iterator", func(t *testing.T) {
		ints := []int{2, 4, 3, 7, 1}
		si := &testSliceIterator[int]{slice: ints}
		ints2 := []int{8, 7, 12}
		si2 := &testSliceIterator[int]{slice: ints2}
		ints3 := []int{0, 1, 2}
		si3 := &testSliceIterator[int]{slice: ints3}

		var eCount int
		input := &testSliceIterator[Iterator[int]]{
			fnHasNext: func() bool { return eCount < 2 },
			fnNext: func() (Iterator[int], error) {
				if eCount >= 2 {
					return nil, io.EOF
				}
				eCount += 1
				if eCount == 1 {
					return si2, nil
				}
				return si3, nil
			},
			fnClose: func() {
				si2.Close()
				si3.Close()
			},
		}

		it := NewStacked1ToNIterator[int](input, si).(*stacked1ToNIterator[int])
		assert.Assert(t, !it.isFinished)
		assert.Equal(t, 0, input.closeCount)
		assert.Equal(t, 0, si.closeCount)
		assert.Equal(t, 0, si2.closeCount)
		assert.Equal(t, 0, si3.closeCount)

		it.Close()
		assert.Assert(t, it.isFinished)
		assert.Equal(t, 1, input.closeCount)
		assert.Equal(t, 1, si.closeCount)
		assert.Equal(t, 1, si2.closeCount)
		assert.Equal(t, 1, si3.closeCount)
	})

	t.Run("calling Close more than once has no effect", func(t *testing.T) {
		ints := []int{2, 4, 3, 7, 1}
		si := &testSliceIterator[int]{slice: ints}
		var eCount int
		input := &testSliceIterator[Iterator[int]]{
			fnHasNext: func() bool { return eCount == 0 },
			fnNext: func() (Iterator[int], error) {
				if eCount > 0 {
					return nil, io.EOF
				}
				eCount += 1
				return si, nil
			},
		}

		it := NewStacked1ToNIterator[int](input, nil).(*stacked1ToNIterator[int])
		assert.Assert(t, it.HasNext())
		assert.Assert(t, !it.isFinished)
		assert.Equal(t, 0, input.closeCount)

		it.Close()
		assert.Assert(t, !it.HasNext())
		assert.Assert(t, it.isFinished)
		assert.Equal(t, 1, input.closeCount)

		it.Close()
		assert.Assert(t, !it.HasNext())
		assert.Assert(t, it.isFinished)
		assert.Equal(t, 1, input.closeCount)
	})

	t.Run("handles input error", func(t *testing.T) {
		var eCount int
		input := &testSliceIterator[Iterator[int]]{
			fnHasNext: func() bool { return eCount == 0 },
			fnNext: func() (Iterator[int], error) {
				eCount += 1
				return nil, errors.New("input failed")
			},
		}

		it := NewStacked1ToNIterator[int](input, nil).(*stacked1ToNIterator[int])
		assert.Assert(t, it.HasNext())
		assert.Assert(t, !it.isFinished)
		assert.Equal(t, 0, input.closeCount)

		_, err := it.Next()
		assert.ErrorContains(t, err, "input failed")
		assert.Assert(t, it.isFinished) // error should cause Close to be called
		assert.Assert(t, !it.HasNext())
	})

	t.Run("handles sub-iterator error", func(t *testing.T) {
		ints := []int{2, 4, 3, 7, 1}
		si := &testSliceIterator[int]{slice: ints, fnNext: func() (int, error) { return 0, errors.New("sub-iterator failed") }}
		it := NewStacked1ToNIterator[int](nil, si).(*stacked1ToNIterator[int])
		assert.Assert(t, it.HasNext())
		assert.Assert(t, !it.isFinished)
		assert.Equal(t, 0, si.closeCount)

		_, err := it.Next()
		assert.ErrorContains(t, err, "sub-iterator failed")
		assert.Assert(t, it.isFinished) // error should cause Close to be called
		assert.Assert(t, !it.HasNext())
		assert.Equal(t, 1, si.closeCount)
	})
}

type testSliceIterator[O any] struct {
	slice      []O
	index      int
	closeCount int
	fnHasNext  func() bool
	fnClose    func()
	fnNext     func() (O, error)
}

var _ Iterator[int] = (*testSliceIterator[int])(nil)

func (tsi *testSliceIterator[O]) HasNext() bool {
	if tsi.fnHasNext != nil {
		return tsi.fnHasNext()
	}
	return tsi.index < len(tsi.slice)
}

func (tsi *testSliceIterator[O]) Next() (O, error) {
	if tsi.fnNext != nil {
		return tsi.fnNext()
	}
	var empty O
	if !tsi.HasNext() {
		return empty, io.EOF
	}
	e := tsi.slice[tsi.index]
	tsi.index += 1
	return e, nil
}

func (tsi *testSliceIterator[O]) Close() {
	tsi.closeCount += 1
	if tsi.fnClose != nil {
		tsi.fnClose()
		return
	}
	tsi.slice = nil
}
