package call

import (
	"fmt"
	"runtime/debug"
)

func WithRecover(fn func() error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("recovered from panic: %+v, stack trace: %s", r, debug.Stack())
		}
	}()
	return fn()
}
