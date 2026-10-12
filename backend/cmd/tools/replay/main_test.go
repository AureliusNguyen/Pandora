package main

import (
	"strings"
	"testing"

	commonv1 "github.com/aureliusnguyen/pandora/backend/gen/go/pandora/common/v1"
	matcherv1 "github.com/aureliusnguyen/pandora/backend/gen/go/pandora/matcher/v1"
)

func lvl(o uint32, side commonv1.Side, p uint32, q uint64) *commonv1.Level {
	return &commonv1.Level{Outcome: o, Side: side, Price: p, Qty: q}
}

func TestDiffBooks(t *testing.T) {
	a := &matcherv1.GetBookResponse{
		MarketId: "mkt_demo", MarketSeq: 10, Status: commonv1.MarketStatus_MARKET_STATUS_OPEN,
		Levels: []*commonv1.Level{lvl(0, commonv1.Side_SIDE_BUY, 55, 10), lvl(1, commonv1.Side_SIDE_SELL, 60, 3)},
	}
	same := &matcherv1.GetBookResponse{
		MarketId: "mkt_demo", MarketSeq: 10, Status: commonv1.MarketStatus_MARKET_STATUS_OPEN,
		Levels: []*commonv1.Level{lvl(1, commonv1.Side_SIDE_SELL, 60, 3), lvl(0, commonv1.Side_SIDE_BUY, 55, 10)},
	} // order must not matter
	if d := diffBooks(a, same); len(d) != 0 {
		t.Fatalf("expected no diff, got %v", d)
	}
	b := &matcherv1.GetBookResponse{
		MarketId: "mkt_demo", MarketSeq: 11, Status: commonv1.MarketStatus_MARKET_STATUS_LOCKED,
		Levels: []*commonv1.Level{lvl(0, commonv1.Side_SIDE_BUY, 55, 9), lvl(0, commonv1.Side_SIDE_SELL, 70, 1)},
	}
	d := diffBooks(a, b)
	joined := strings.Join(d, "\n")
	for _, want := range []string{"market_seq 10 != 11", "status", "0 BUY 55: 10 != 9", "1 SELL 60: only in replay", "0 SELL 70: only in live"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("diff missing %q:\n%s", want, joined)
		}
	}
	if len(d) != 5 {
		t.Fatalf("expected 5 diff lines, got %d:\n%s", len(d), joined)
	}
}

// Usage errors exit 2 before any broker is contacted.
func TestRunUsageErrors(t *testing.T) {
	for name, args := range map[string][]string{
		"unknown flag":   {"--nope"},
		"extra argument": {"extra"},
		"empty brokers":  {"--brokers", " , "},
		"zero timeout":   {"--timeout", "0s"},
	} {
		var out, errOut strings.Builder
		if code := run(args, &out, &errOut); code != exitError {
			t.Errorf("%s: exit %d, want %d (stderr: %s)", name, code, exitError, errOut.String())
		}
		if out.Len() != 0 {
			t.Errorf("%s: unexpected stdout %q", name, out.String())
		}
	}
}
