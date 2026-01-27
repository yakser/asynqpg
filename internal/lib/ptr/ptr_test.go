package ptr

import (
	"testing"
)

func TestGet_Int(t *testing.T) {
	v := 42
	p := Get(v)
	if p == nil {
		t.Fatal("expected non-nil pointer")
	}
	if *p != v {
		t.Fatalf("expected %d, got %d", v, *p)
	}
}

const testHelloStr = "hello"

func TestGet_String(t *testing.T) {
	v := testHelloStr
	p := Get(v)
	if p == nil {
		t.Fatal("expected non-nil pointer")
	}
	if *p != v {
		t.Fatalf("expected %q, got %q", v, *p)
	}
}

type testStruct struct {
	A int
	B string
}

func TestGet_Struct(t *testing.T) {
	v := testStruct{A: 1, B: "test"}
	p := Get(v)
	if p == nil {
		t.Fatal("expected non-nil pointer")
	}
	if *p != v {
		t.Fatalf("expected %+v, got %+v", v, *p)
	}
}

func TestGet_ZeroValue(t *testing.T) {
	p := Get(0)
	if p == nil {
		t.Fatal("expected non-nil pointer")
	}
	if *p != 0 {
		t.Fatalf("expected 0, got %d", *p)
	}
}

func TestDerefOrDefault_NonNil(t *testing.T) {
	v := 42
	result := DerefOrDefault(&v, 0)
	if result != 42 {
		t.Fatalf("expected 42, got %d", result)
	}
}

func TestDerefOrDefault_Nil(t *testing.T) {
	result := DerefOrDefault[int](nil, 99)
	if result != 99 {
		t.Fatalf("expected 99, got %d", result)
	}
}

func TestDerefOrDefault_ZeroValue(t *testing.T) {
	v := 0
	result := DerefOrDefault(&v, 99)
	if result != 0 {
		t.Fatalf("expected 0 (dereferenced), got %d", result)
	}
}

func TestDerefOrDefault_String(t *testing.T) {
	v := testHelloStr
	result := DerefOrDefault(&v, "default")
	if result != testHelloStr {
		t.Fatalf("expected %q, got %q", testHelloStr, result)
	}
}

func TestDerefOrDefault_NilString(t *testing.T) {
	result := DerefOrDefault[string](nil, "default")
	if result != "default" {
		t.Fatalf("expected %q, got %q", "default", result)
	}
}
