package outbox

import (
	"bytes"
	"crypto/rand"
	"io"
	"testing"
	"time"
)

type fixedClock struct {
	now time.Time
}

func (c fixedClock) Now() time.Time {
	return c.now
}

type sequenceClock struct {
	times []time.Time
	index int
}

func (c *sequenceClock) Now() time.Time {
	if len(c.times) == 0 {
		return time.Time{}
	}
	if c.index >= len(c.times) {
		return c.times[len(c.times)-1]
	}
	t := c.times[c.index]
	c.index++

	return t
}

func TestID_StringRoundTrip(t *testing.T) {
	gen := newUUIDv7GeneratorWithRand(fixedClock{now: time.Unix(1, 0)}, bytes.NewReader(bytes.Repeat([]byte{0x42}, 64)))
	id, err := gen.New()
	if err != nil {
		t.Fatalf("new id: %v", err)
	}

	parsed, err := ParseID(id.String())
	if err != nil {
		t.Fatalf("parse id: %v", err)
	}
	if parsed != id {
		t.Fatalf("expected round-trip to match")
	}
}

func TestParseID_Invalid(t *testing.T) {
	cases := []string{
		"",
		"not-a-uuid",
		"00000000-0000-0000-0000-00000000000",
		"000000000000000000000000000000000",
		"00000000_0000_0000_0000_000000000000",
	}
	for _, value := range cases {
		if _, err := ParseID(value); err == nil {
			t.Fatalf("expected error for %q", value)
		}
	}
}

func TestUUIDv7Generator_VersionVariant(t *testing.T) {
	gen := newUUIDv7GeneratorWithRand(fixedClock{now: time.Unix(10, 0)}, bytes.NewReader(bytes.Repeat([]byte{0x11}, 64)))
	id, err := gen.New()
	if err != nil {
		t.Fatalf("new id: %v", err)
	}

	version := id[6] >> 4
	if version != 0x7 {
		t.Fatalf("expected version 7, got %x", version)
	}

	variant := id[8] >> 6
	if variant != 0x2 {
		t.Fatalf("expected variant 10, got %x", variant)
	}
}

func TestUUIDv7Generator_Monotonic(t *testing.T) {
	gen := newUUIDv7GeneratorWithRand(fixedClock{now: time.Unix(10, 0)}, bytes.NewReader(bytes.Repeat([]byte{0x22}, 128)))
	id1, err := gen.New()
	if err != nil {
		t.Fatalf("new id1: %v", err)
	}
	id2, err := gen.New()
	if err != nil {
		t.Fatalf("new id2: %v", err)
	}

	if bytes.Compare(id1[:], id2[:]) >= 0 {
		t.Fatalf("expected id2 to be greater than id1")
	}
}

func TestUUIDv7GeneratorClockBackwards(t *testing.T) {
	t1 := time.Unix(10, 0)
	t0 := time.Unix(9, 0)
	clock := &sequenceClock{times: []time.Time{t1, t0}}
	gen := newUUIDv7GeneratorWithRand(clock, bytes.NewReader(bytes.Repeat([]byte{0x42}, 64)))

	id1, err := gen.New()
	if err != nil {
		t.Fatalf("new id1: %v", err)
	}
	id2, err := gen.New()
	if err != nil {
		t.Fatalf("new id2: %v", err)
	}

	if bytes.Compare(id1[:], id2[:]) >= 0 {
		t.Fatalf("expected id2 to be greater than id1 on clock rollback")
	}
}

func TestUUIDv7GeneratorSequenceOverflow(t *testing.T) {
	base := time.Unix(100, 0)
	clock := &sequenceClock{times: []time.Time{base, base.Add(time.Millisecond)}}
	gen := newUUIDv7GeneratorWithRand(clock, bytes.NewReader(bytes.Repeat([]byte{0x33}, 64)))
	gen.lastMS = base.UnixMilli()
	gen.seq = randAMask

	id, err := gen.New()
	if err != nil {
		t.Fatalf("new id: %v", err)
	}

	ts := idTimestampMillis(id)
	if ts <= base.UnixMilli() {
		t.Fatalf("expected timestamp to advance, got %d", ts)
	}
	if gen.lastMS <= base.UnixMilli() {
		t.Fatalf("expected generator to advance lastMS")
	}
}

func TestIDScanBinary(t *testing.T) {
	gen := newUUIDv7GeneratorWithRand(fixedClock{now: time.Unix(10, 0)}, bytes.NewReader(bytes.Repeat([]byte{0x33}, 64)))
	id, err := gen.New()
	if err != nil {
		t.Fatalf("new id: %v", err)
	}

	var scanned ID
	if err := scanned.Scan(id[:]); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if scanned != id {
		t.Fatalf("scan did not match")
	}
}

func newUUIDv7GeneratorWithRand(clock Clock, r io.Reader) *UUIDv7Generator {
	if clock == nil {
		clock = SystemClock{}
	}
	if r == nil {
		r = rand.Reader
	}

	return &UUIDv7Generator{clock: clock, rand: r}
}

func idTimestampMillis(id ID) int64 {
	return int64(id[0])<<shift40 |
		int64(id[1])<<shift32 |
		int64(id[2])<<shift24 |
		int64(id[3])<<shift16 |
		int64(id[4])<<shift8 |
		int64(id[5])
}
