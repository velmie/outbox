package outbox

import "time"

// Clock abstracts time for deterministic tests.
type Clock interface {
	// Now returns the current time.
	Now() time.Time
}

// SystemClock uses the system time in UTC.
type SystemClock struct{}

// Now returns the current UTC time.
func (SystemClock) Now() time.Time {
	return time.Now().UTC()
}
