package consumer

import "errors"

var (
	ErrTaskHandlerAlreadyRegistered = errors.New("task handler already registered")
)
