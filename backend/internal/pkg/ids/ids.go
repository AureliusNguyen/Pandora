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
