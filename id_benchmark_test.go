package outbox

import (
	"testing"
	"time"
)

func BenchmarkUUIDv7Generator(b *testing.B) {
	gen := NewUUIDv7Generator(SystemClock{})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := gen.New(); err != nil {
			b.Fatalf("new id: %v", err)
		}
	}
}

func BenchmarkIDString(b *testing.B) {
	gen := NewUUIDv7Generator(fixedClock{now: time.Unix(1, 0)})
	id, err := gen.New()
	if err != nil {
		b.Fatalf("new id: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = id.String()
	}
}
