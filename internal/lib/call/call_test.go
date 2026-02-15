package call

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func funcThatPanics(message string) error {
	panic(message)
}

func TestWithRecover(t *testing.T) {
	t.Parallel()

	err := WithRecover(func() error {
		return funcThatPanics("test panic")
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "test panic")
}

func TestWithRecover_NoPanic(t *testing.T) {
	t.Parallel()

	err := WithRecover(func() error {
		return nil
	})

	assert.NoError(t, err)
}

func TestWithRecover_ReturnedError(t *testing.T) {
	t.Parallel()

	want := errors.New("some error")

	err := WithRecover(func() error {
		return want
	})

	assert.ErrorIs(t, err, want)
}

func TestWithRecover_PanicWithNil(t *testing.T) {
	t.Parallel()

	err := WithRecover(func() error {
		panic(nil)
	})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "recovered from panic")
}
