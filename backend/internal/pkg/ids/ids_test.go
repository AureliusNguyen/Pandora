package ids_test

import (
	"sync"
	"testing"

	"github.com/oklog/ulid/v2"

	"github.com/aureliusnguyen/pandora/backend/internal/pkg/ids"
)

func TestNewIsValidULID(t *testing.T) {
	id := ids.New()
	if len(id) != 26 {
		t.Fatalf("len(%q) = %d, want 26", id, len(id))
	}
	if _, err := ulid.ParseStrict(id); err != nil {
		t.Fatalf("ParseStrict(%q): %v", id, err)
	}
}

func TestNewIsStrictlyIncreasing(t *testing.T) {
	prev := ids.New()
	for i := 0; i < 10000; i++ {
		id := ids.New()
		if id <= prev {
			t.Fatalf("id %d: %q <= previous %q", i, id, prev)
		}
		prev = id
	}
}

func TestNewConcurrentUnique(t *testing.T) {
	const workers, per = 8, 2000
	out := make(chan string, workers*per)
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range per {
				out <- ids.New()
			}
		}()
	}
	wg.Wait()
	close(out)
	seen := make(map[string]struct{}, workers*per)
	for id := range out {
		if _, dup := seen[id]; dup {
			t.Fatalf("duplicate id %q", id)
		}
		seen[id] = struct{}{}
	}
}

func TestValid(t *testing.T) {
	cases := []struct {
		id   string
		want bool
	}{
		{ids.New(), true},
		{"01ARZ3NDEKTSV4RRFFQ69G5FAV", true},
		{"01arz3ndektsv4rrffq69g5fav", true},  // case-insensitive
		{"7ZZZZZZZZZZZZZZZZZZZZZZZZZ", true},  // largest encodable
		{"8ZZZZZZZZZZZZZZZZZZZZZZZZZ", false}, // first char > 7 overflows 128 bits
		{"", false},
		{"given-id", false},
		{"01ARZ3NDEKTSV4RRFFQ69G5FA", false},   // 25 chars
		{"01ARZ3NDEKTSV4RRFFQ69G5FAVV", false}, // 27 chars
		{"01ARZ3NDEKTSV4RRFFQ69G5FAI", false},  // I is not Crockford
		{"01ARZ3NDEKTSV4RRFFQ69G5FAL", false},  // L is not Crockford
		{"01ARZ3NDEKTSV4RRFFQ69G5FAO", false},  // O is not Crockford
		{"01ARZ3NDEKTSV4RRFFQ69G5FAU", false},  // U is not Crockford
		{"01ARZ3NDEKTSV4RRFFQ69G5FA-", false},
		{"01ARZ3NDEKTSV4RRFFQ69G5F" + "\u00e9", false}, // 26 bytes but non-ASCII
	}
	for _, tc := range cases {
		if got := ids.Valid(tc.id); got != tc.want {
			t.Errorf("Valid(%q) = %v, want %v", tc.id, got, tc.want)
		}
	}
}
