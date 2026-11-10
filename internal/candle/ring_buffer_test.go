package candle

import "testing"

func TestNewRingBuffer_PanicsOnZero(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for capacity 0")
		}
	}()
	NewRingBuffer[int](0)
}

func TestPush_And_PeekLatest(t *testing.T) {
	rb := NewRingBuffer[int](3)

	if _, ok := rb.PeekLatest(); ok {
		t.Fatal("PeekLatest should return false on empty buffer")
	}

	rb.Push(1)
	v, ok := rb.PeekLatest()
	if !ok || v != 1 {
		t.Fatalf("expected 1, got %d", v)
	}

	rb.Push(2)
	rb.Push(3)
	v2, _ := rb.PeekLatest()
	if v2 != 3 {
		t.Fatalf("expected 3, got %d", v2)
	}
}

func TestLen_Cap(t *testing.T) {
	rb := NewRingBuffer[int](5)
	if rb.Cap() != 5 {
		t.Fatalf("cap: want 5, got %d", rb.Cap())
	}
	if rb.Len() != 0 {
		t.Fatalf("len: want 0, got %d", rb.Len())
	}
	rb.Push(1)
	rb.Push(2)
	if rb.Len() != 2 {
		t.Fatalf("len: want 2, got %d", rb.Len())
	}
}

func TestWrap_Overwrite(t *testing.T) {
	rb := NewRingBuffer[int](3)
	rb.Push(1)
	rb.Push(2)
	rb.Push(3)
	rb.Push(4)

	if rb.Len() != 3 {
		t.Fatalf("len: want 3, got %d", rb.Len())
	}
	s := rb.Slice()
	want := []int{2, 3, 4}
	for i := range want {
		if s[i] != want[i] {
			t.Fatalf("Slice[%d]: want %d, got %d", i, want[i], s[i])
		}
	}
}

func TestSlice_OldestToNewest(t *testing.T) {
	rb := NewRingBuffer[int](5)
	for i := 1; i <= 5; i++ {
		rb.Push(i)
	}
	s := rb.Slice()
	for i := range s {
		if s[i] != i+1 {
			t.Fatalf("Slice[%d]: want %d, got %d", i, i+1, s[i])
		}
	}
}

func TestSliceDesc_NewestToOldest(t *testing.T) {
	rb := NewRingBuffer[int](5)
	for i := 1; i <= 5; i++ {
		rb.Push(i)
	}
	d := rb.SliceDesc()
	for i := range d {
		want := 5 - i
		if d[i] != want {
			t.Fatalf("SliceDesc[%d]: want %d, got %d", i, want, d[i])
		}
	}
}

func TestSlice_Empty(t *testing.T) {
	rb := NewRingBuffer[int](3)
	if s := rb.Slice(); s != nil {
		t.Fatalf("Slice: want nil, got %v", s)
	}
	if d := rb.SliceDesc(); d != nil {
		t.Fatalf("SliceDesc: want nil, got %v", d)
	}
}

func TestWrap_MultipleCycles(t *testing.T) {
	rb := NewRingBuffer[int](3)
	for i := 1; i <= 10; i++ {
		rb.Push(i)
	}
	s := rb.Slice()
	if len(s) != 3 {
		t.Fatalf("len: want 3, got %d", len(s))
	}
	want := []int{8, 9, 10}
	for i := range want {
		if s[i] != want[i] {
			t.Fatalf("Slice[%d]: want %d, got %d", i, want[i], s[i])
		}
	}
	latest, _ := rb.PeekLatest()
	if latest != 10 {
		t.Fatalf("PeekLatest: want 10, got %d", latest)
	}
}

func TestCapacity_One(t *testing.T) {
	rb := NewRingBuffer[string](1)
	rb.Push("a")
	rb.Push("b")
	if rb.Len() != 1 {
		t.Fatalf("len: want 1, got %d", rb.Len())
	}
	v, _ := rb.PeekLatest()
	if v != "b" {
		t.Fatalf("PeekLatest: want b, got %s", v)
	}
}

func TestOldest(t *testing.T) {
	rb := NewRingBuffer[int](3)
	if _, ok := rb.Oldest(); ok {
		t.Fatal("Oldest should return false on empty buffer")
	}
	rb.Push(1)
	rb.Push(2)
	rb.Push(3)
	rb.Push(4)
	got, ok := rb.Oldest()
	if !ok || got != 2 {
		t.Fatalf("Oldest: want 2, got %d (ok=%v)", got, ok)
	}
}

func TestReset(t *testing.T) {
	rb := NewRingBuffer[int](3)
	rb.Push(1)
	rb.Push(2)
	rb.Reset()
	if rb.Len() != 0 {
		t.Fatalf("len after Reset: want 0, got %d", rb.Len())
	}
	if _, ok := rb.Oldest(); ok {
		t.Fatal("Oldest should return false after Reset")
	}
	rb.Push(7)
	latest, _ := rb.PeekLatest()
	if latest != 7 {
		t.Fatalf("PeekLatest after Reset: want 7, got %d", latest)
	}
}

func TestAtAndSetAt(t *testing.T) {
	rb := NewRingBuffer[int](3)
	rb.Push(1)
	rb.Push(2)
	rb.Push(3)
	rb.Push(4)
	if got, ok := rb.At(0); !ok || got != 2 {
		t.Fatalf("At(0): want 2, got %d (ok=%v)", got, ok)
	}
	if got, ok := rb.At(2); !ok || got != 4 {
		t.Fatalf("At(2): want 4, got %d (ok=%v)", got, ok)
	}
	if _, ok := rb.At(3); ok {
		t.Fatal("At out of range should return false")
	}
	if !rb.SetAt(1, 9) {
		t.Fatal("SetAt in range should succeed")
	}
	if got, _ := rb.At(1); got != 9 {
		t.Fatalf("At(1) after SetAt: want 9, got %d", got)
	}
	if rb.SetAt(3, 0) {
		t.Fatal("SetAt out of range should fail")
	}
}

func TestUpdateLatestAndSetLatest(t *testing.T) {
	rb := NewRingBuffer[int](2)
	if rb.UpdateLatest(func(*int) { t.Fatal("callback must not run on empty buffer") }) {
		t.Fatal("UpdateLatest on empty buffer should return false")
	}
	rb.Push(1)
	if !rb.UpdateLatest(func(v *int) { *v += 10 }) {
		t.Fatal("UpdateLatest on non-empty buffer should return true")
	}
	if got, _ := rb.PeekLatest(); got != 11 {
		t.Fatalf("PeekLatest after UpdateLatest: want 11, got %d", got)
	}
	if !rb.SetLatest(20) {
		t.Fatal("SetLatest on non-empty buffer should return true")
	}
	if got, _ := rb.PeekLatest(); got != 20 {
		t.Fatalf("PeekLatest after SetLatest: want 20, got %d", got)
	}
}
