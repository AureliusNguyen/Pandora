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
