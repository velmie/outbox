package outbox

import (
	"crypto/rand"
	"database/sql/driver"
	"encoding/hex"
	"fmt"
	"io"
	"sync"
	"time"
)

const (
	uuidRawLength  = 16
	uuidHexLength  = 32
	uuidTextLength = 36

	uuidVersionByte = 0x70
	uuidVariantBits = 0x80
	uuidVariantMask = 0x3f
	randAMask       = 0x0fff
	randANibbleMask = 0x0f
	randBytesLength = 8

	shift40 = 40
	shift32 = 32
	shift24 = 24
	shift16 = 16
	shift8  = 8

	clockSleepStep = 1 * time.Millisecond
	clockSleepMax  = 100 * time.Millisecond
)

// ID is a UUID v7 identifier stored as 16 raw bytes.
//
//nolint:recvcheck // Scan requires a pointer receiver, Value uses value receiver for driver.Valuer.
type ID [16]byte

// Bytes returns a copy of the raw 16 bytes.
func (id ID) Bytes() []byte {
	out := make([]byte, len(id))
	copy(out, id[:])

	return out
}

// IsZero reports whether the ID is all zeros.
func (id ID) IsZero() bool {
	return id == ID{}
}

// String returns the canonical UUID string representation.
func (id ID) String() string {
	var hexbuf [uuidHexLength]byte
	hex.Encode(hexbuf[:], id[:])
	var out [uuidTextLength]byte
	copy(out[0:8], hexbuf[0:8])
	out[8] = '-'
	copy(out[9:13], hexbuf[8:12])
	out[13] = '-'
	copy(out[14:18], hexbuf[12:16])
	out[18] = '-'
	copy(out[19:23], hexbuf[16:20])
	out[23] = '-'
	copy(out[24:36], hexbuf[20:32])

	return string(out[:])
}

// MarshalText implements encoding.TextMarshaler.
func (id ID) MarshalText() ([]byte, error) {
	return []byte(id.String()), nil
}

// UnmarshalText implements encoding.TextUnmarshaler.
func (id *ID) UnmarshalText(text []byte) error {
	parsed, err := ParseID(string(text))
	if err != nil {
		return err
	}
	*id = parsed

	return nil
}

// Scan implements sql.Scanner for BINARY(16) or textual UUIDs.
// NULL is treated as ErrInvalidID.
func (id *ID) Scan(src any) error {
	switch value := src.(type) {
	case nil:
		return ErrInvalidID
	case []byte:
		return id.scanBytes(value)
	case string:
		parsed, err := ParseID(value)
		if err != nil {
			return err
		}
		*id = parsed

		return nil
	default:
		return fmt.Errorf("outbox: unsupported id type %T: %w", src, ErrInvalidID)
	}
}

// Value implements driver.Valuer for BINARY(16).
func (id ID) Value() (driver.Value, error) {
	return id[:], nil
}

// ParseID parses a UUID string (canonical or 32 hex) into an ID.
func ParseID(value string) (ID, error) {
	var hexbuf [uuidHexLength]byte
	switch len(value) {
	case uuidHexLength:
		copy(hexbuf[:], value)
	case uuidTextLength:
		if value[8] != '-' || value[13] != '-' || value[18] != '-' || value[23] != '-' {
			return ID{}, ErrInvalidID
		}
		copy(hexbuf[0:8], value[0:8])
		copy(hexbuf[8:12], value[9:13])
		copy(hexbuf[12:16], value[14:18])
		copy(hexbuf[16:20], value[19:23])
		copy(hexbuf[20:32], value[24:36])
	default:
		return ID{}, ErrInvalidID
	}

	var id ID
	if _, err := hex.Decode(id[:], hexbuf[:]); err != nil {
		return ID{}, ErrInvalidID
	}

	return id, nil
}

func (id *ID) scanBytes(value []byte) error {
	switch len(value) {
	case uuidRawLength:
		copy(id[:], value)

		return nil
	case uuidHexLength, uuidTextLength:
		parsed, err := ParseID(string(value))
		if err != nil {
			return err
		}
		*id = parsed

		return nil
	default:
		return ErrInvalidID
	}
}

// IDGenerator creates new identifiers.
type IDGenerator interface {
	// New returns a new identifier.
	New() (ID, error)
}

// UUIDv7Generator produces monotonic UUID v7 identifiers.
type UUIDv7Generator struct {
	mu     sync.Mutex
	clock  Clock
	rand   io.Reader
	lastMS int64
	seq    uint16
}

// NewUUIDv7Generator creates a generator using the provided clock.
func NewUUIDv7Generator(clock Clock) *UUIDv7Generator {
	if clock == nil {
		clock = SystemClock{}
	}

	return &UUIDv7Generator{clock: clock, rand: rand.Reader}
}

// New creates a new UUID v7 identifier.
func (g *UUIDv7Generator) New() (ID, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	now, err := g.nextTimestamp()
	if err != nil {
		return ID{}, err
	}

	randBytes, err := g.randBlock()
	if err != nil {
		return ID{}, err
	}

	id := buildUUIDv7(now, g.seq, randBytes)

	return id, nil
}

func (g *UUIDv7Generator) nextTimestamp() (int64, error) {
	now := g.clock.Now().UnixMilli()
	if now < g.lastMS {
		now = g.lastMS
	}
	if now != g.lastMS {
		g.lastMS = now
		seq, err := g.randSeq()
		if err != nil {
			return 0, err
		}
		g.seq = seq

		return now, nil
	}

	g.seq++
	if g.seq <= randAMask {
		return now, nil
	}

	now = g.waitNextMillisecond(now)
	seq, err := g.randSeq()
	if err != nil {
		return 0, err
	}
	g.lastMS = now
	g.seq = seq

	return now, nil
}

func (g *UUIDv7Generator) waitNextMillisecond(now int64) int64 {
	for now <= g.lastMS {
		drift := g.lastMS - now
		if drift > 0 {
			sleepFor := time.Duration(drift) * time.Millisecond
			if sleepFor > clockSleepMax {
				sleepFor = clockSleepMax
			}
			time.Sleep(sleepFor)
		} else {
			time.Sleep(clockSleepStep)
		}
		now = g.clock.Now().UnixMilli()
	}

	return now
}

func (g *UUIDv7Generator) randBlock() ([randBytesLength]byte, error) {
	var randBytes [randBytesLength]byte
	if _, err := io.ReadFull(g.rand, randBytes[:]); err != nil {
		return randBytes, err
	}
	randBytes[0] = (randBytes[0] & uuidVariantMask) | uuidVariantBits

	return randBytes, nil
}

func buildUUIDv7(now int64, randA uint16, randBytes [randBytesLength]byte) ID {
	var id ID
	id[0] = byte(now >> shift40)
	id[1] = byte(now >> shift32)
	id[2] = byte(now >> shift24)
	id[3] = byte(now >> shift16)
	id[4] = byte(now >> shift8)
	id[5] = byte(now)

	randA &= randAMask
	id[6] = byte(uuidVersionByte | (randA>>shift8)&randANibbleMask)
	id[7] = byte(randA)
	copy(id[8:], randBytes[:])

	return id
}

func (g *UUIDv7Generator) randSeq() (uint16, error) {
	var buf [2]byte
	if _, err := io.ReadFull(g.rand, buf[:]); err != nil {
		return 0, err
	}
	seq := uint16(buf[0])<<shift8 | uint16(buf[1])

	return seq & randAMask, nil
}
