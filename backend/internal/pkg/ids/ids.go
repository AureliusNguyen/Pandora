// Package ids generates lexicographically sortable unique identifiers (ULIDs).
package ids

import (
	"crypto/rand"
	"sync"
	"time"

	"github.com/oklog/ulid/v2"
)

var (
	mu      sync.Mutex
	entropy = ulid.Monotonic(rand.Reader, 0)
)

// New returns a 26-character ULID. IDs from one process are strictly
// increasing (including within one millisecond) as long as the wall clock
// does not step backwards. Safe for concurrent use.
func New() string {
	mu.Lock()
	defer mu.Unlock()
	return ulid.MustNew(ulid.Timestamp(time.Now()), entropy).String()
}

// Valid reports whether s is a 26-character Crockford base32 ULID
// (digits and A-Z without I, L, O, U, case-insensitive) whose first
// character is 0..7 so the value fits in 128 bits. It accepts exactly what
// ulid.ParseStrict accepts, without allocating.
func Valid(s string) bool {
	if len(s) != 26 || s[0] > '7' {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= '0' && c <= '9':
		case c >= 'a' && c <= 'z':
			c -= 'a' - 'A'
			fallthrough
		case c >= 'A' && c <= 'Z':
			if c == 'I' || c == 'L' || c == 'O' || c == 'U' {
				return false
			}
		default:
			return false
		}
	}
	return true
}
