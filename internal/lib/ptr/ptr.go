package ptr

func Get[T any](v T) *T {
	return &v
}

func DerefOrDefault[T any](v *T, defaultValue T) T {
	if v == nil {
		return defaultValue
	}
	return *v
}
