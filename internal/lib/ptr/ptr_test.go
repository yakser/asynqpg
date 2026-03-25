package ptr

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGet_Int(t *testing.T) {
	t.Parallel()

	v := 42
	p := Get(v)
	require.NotNil(t, p)
	require.Equal(t, v, *p)
}

const testHelloStr = "hello"

func TestGet_String(t *testing.T) {
	t.Parallel()

	v := testHelloStr
	p := Get(v)
	require.NotNil(t, p)
	require.Equal(t, v, *p)
}

type testStruct struct {
	A int
	B string
}

func TestGet_Struct(t *testing.T) {
	t.Parallel()

	v := testStruct{A: 1, B: "test"}
	p := Get(v)
	require.NotNil(t, p)
	require.Equal(t, v, *p)
}

func TestGet_ZeroValue(t *testing.T) {
	t.Parallel()

	p := Get(0)
	require.NotNil(t, p)
	require.Equal(t, 0, *p)
}

func TestDerefOrDefault_NonNil(t *testing.T) {
	t.Parallel()

	v := 42
	result := DerefOrDefault(&v, 0)
	require.Equal(t, 42, result)
}

func TestDerefOrDefault_Nil(t *testing.T) {
	t.Parallel()

	result := DerefOrDefault[int](nil, 99)
	require.Equal(t, 99, result)
}

func TestDerefOrDefault_ZeroValue(t *testing.T) {
	t.Parallel()

	v := 0
	result := DerefOrDefault(&v, 99)
	require.Equal(t, 0, result)
}

func TestDerefOrDefault_String(t *testing.T) {
	t.Parallel()

	v := testHelloStr
	result := DerefOrDefault(&v, "default")
	require.Equal(t, testHelloStr, result)
}

func TestDerefOrDefault_NilString(t *testing.T) {
	t.Parallel()

	result := DerefOrDefault[string](nil, "default")
	require.Equal(t, "default", result)
}
