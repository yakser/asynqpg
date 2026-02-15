package consumer

// MiddlewareFunc wraps a TaskHandler with additional behavior.
// First registered middleware is the outermost (runs first on the way in, last on the way out).
type MiddlewareFunc func(TaskHandler) TaskHandler

// buildHandlerChain wraps a handler with global and per-task middleware.
// Per-task middleware is applied first (innermost), then global middleware (outermost).
// The resulting execution order is: global[0] → global[1] → ... → perTask[0] → perTask[1] → ... → handler.
func buildHandlerChain(handler TaskHandler, global, perTask []MiddlewareFunc) TaskHandler {
	if len(global) == 0 && len(perTask) == 0 {
		return handler
	}

	wrapped := handler

	// Apply per-task middleware in reverse order so first-registered = innermost of per-task group.
	for i := len(perTask) - 1; i >= 0; i-- {
		if perTask[i] != nil {
			wrapped = perTask[i](wrapped)
		}
	}

	// Apply global middleware in reverse order so first-registered = outermost.
	for i := len(global) - 1; i >= 0; i-- {
		if global[i] != nil {
			wrapped = global[i](wrapped)
		}
	}

	return wrapped
}
