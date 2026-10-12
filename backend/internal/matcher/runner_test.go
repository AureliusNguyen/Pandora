package matcher

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"google.golang.org/protobuf/proto"

	enginev1 "github.com/aureliusnguyen/pandora/backend/gen/go/pandora/engine/v1"
	"github.com/aureliusnguyen/pandora/backend/internal/engine"
)

type fatalRec struct {
	mu    sync.Mutex
	calls []string
}

func (f *fatalRec) hook(msg string, _ error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, msg)
}

func (f *fatalRec) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

func newRunnerT(t *testing.T) (*Runner, *FakeWAL, *fatalRec, context.CancelFunc) {
	t.Helper()
	w := NewFakeWAL()
	fr := &fatalRec{}
	r := NewRunner(0, engine.New(0), w, NewMetrics(prometheus.NewRegistry()), slog.Default())
	r.Fatal = fr.hook
	r.Now = func() int64 { return 42 }
	// Real backoffs are seconds; tests only check the retry counts.
	r.walBackoff = backoff{base: time.Millisecond, max: time.Millisecond}
	r.publishBackoff = backoff{base: time.Millisecond, max: time.Millisecond}
	r.shutdownGrace = 20 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	r.Start(ctx)
	t.Cleanup(func() { cancel(); <-r.Done() })
	return r, w, fr, cancel
}

// waitFor polls cond for up to five seconds.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestRunnerStampsAppendsAppliesPublishes(t *testing.T) {
	r, w, _, _ := newRunnerT(t)
	ctx := context.Background()
	res, err := r.Submit(ctx, &enginev1.Command{Body: createCmd(0).Body})
	if err != nil || res.Seq != 1 || res.EngineTime != 42 || len(res.Events) != 1 {
		t.Fatalf("create: res=%+v err=%v", res, err)
	}
	res, err = r.Submit(ctx, &enginev1.Command{Body: submitCmd(0, "a", 40).Body})
	if err != nil || res.Seq != 2 || len(res.Events) != 2 {
		t.Fatalf("submit: res=%+v err=%v", res, err)
	}
	if len(w.Records[0]) != 2 || w.EventCount() != 3 || w.Events[0].Key != "mkt_demo" || w.Events[0].Partition != 0 {
		t.Fatalf("wal=%d events=%d", len(w.Records[0]), w.EventCount())
	}
	// the WAL record carries the stamped header
	var stored enginev1.Command
	if err := protoUnmarshal(w.Records[0][1], &stored); err != nil || stored.Seq != 2 || stored.Epoch != Epoch || stored.Partition != 0 || stored.EngineTimeUnixNanos != 42 {
		t.Fatalf("stored header wrong: %+v err=%v", &stored, err)
	}
	// the published event is the one returned to the caller
	var ev enginev1.Event
	if err := protoUnmarshal(w.Events[1].Value, &ev); err != nil || ev.Seq != 2 || ev.Idx != 0 || ev.GetOrderAccepted() == nil {
		t.Fatalf("published event wrong: %+v err=%v", &ev, err)
	}
	if !r.HasMarket("mkt_demo") || r.HasMarket("x") {
		t.Fatal("HasMarket")
	}
	levels, seq, _, ok := r.Book("mkt_demo")
	if !ok || seq != 2 || len(levels) != 1 {
		t.Fatalf("Book: ok=%v seq=%d levels=%d", ok, seq, len(levels))
	}
	if got := testutil.ToFloat64(r.m.Commands.WithLabelValues("submit_order")); got != 1 {
		t.Fatalf("commands_total{submit_order}=%v", got)
	}
	if got := testutil.ToFloat64(r.m.LastSeq.WithLabelValues("0")); got != 2 {
		t.Fatalf("last_seq=%v", got)
	}
}

func TestRunnerBatchesConcurrentRequests(t *testing.T) {
	r, w, _, _ := newRunnerT(t)
	ctx := context.Background()
	if _, err := r.Submit(ctx, &enginev1.Command{Body: createCmd(0).Body}); err != nil {
		t.Fatal(err)
	}
	const n = 600
	var wg sync.WaitGroup
	seqs := make([]uint64, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			res, err := r.Submit(ctx, &enginev1.Command{Body: submitCmd(0, "o"+itoa(i), uint32(20+i%60)).Body})
			if err != nil {
				t.Error(err)
				return
			}
			seqs[i] = res.Seq
		}(i)
	}
	wg.Wait()
	seen := make(map[uint64]bool, n)
	for _, s := range seqs {
		if s < 2 || s > n+1 || seen[s] {
			t.Fatalf("seq %d out of range or duplicated", s)
		}
		seen[s] = true
	}
	if w.Appends >= n+1 {
		t.Fatalf("no batching happened: %d appends for %d commands", w.Appends, n+1)
	}
	if len(w.Records[0]) != n+1 {
		t.Fatalf("wal has %d records", len(w.Records[0]))
	}
	if r.eng.LastSeq() != n+1 {
		t.Fatalf("engine last seq %d", r.eng.LastSeq())
	}
	t.Logf("%d commands in %d appends", n+1, w.Appends)
}

func TestRunnerBatchIsCappedAtMaxBatch(t *testing.T) {
	r, w, _, _ := newRunnerT(t)
	ctx := context.Background()
	if _, err := r.Submit(ctx, &enginev1.Command{Body: createCmd(0).Body}); err != nil {
		t.Fatal(err)
	}
	// Block the loop inside a publish retry with a one-request batch so
	// every later request queues up behind it.
	w.SetFailPublish(errors.New("events down"))
	var wg sync.WaitGroup
	submit := func(i int) {
		defer wg.Done()
		if _, err := r.Submit(ctx, &enginev1.Command{Body: submitCmd(0, "o"+itoa(i), 50).Body}); err != nil {
			t.Error(err)
		}
	}
	wg.Add(1)
	go submit(0)
	waitFor(t, "a publish failure", func() bool { return testutil.ToFloat64(r.m.PublishFailures) >= 1 })
	const n = 3 * MaxBatch
	for i := 1; i <= n; i++ {
		wg.Add(1)
		go submit(i)
	}
	waitFor(t, "the queue to fill", func() bool { return len(r.reqs) == n })
	w.SetFailPublish(nil)
	wg.Wait()
	if len(w.Records[0]) != n+2 {
		t.Fatalf("wal has %d records", len(w.Records[0]))
	}
	// create, the blocked single, then the queue drained in full batches.
	want := []int{1, 1, MaxBatch, MaxBatch, MaxBatch}
	got := w.BatchSizes()
	if len(got) != len(want) {
		t.Fatalf("append sizes %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("append sizes %v, want %v", got, want)
		}
	}
}

func TestRunnerProduceFailureFailsBatchAndCallsFatal(t *testing.T) {
	r, w, fr, _ := newRunnerT(t)
	ctx := context.Background()
	if _, err := r.Submit(ctx, &enginev1.Command{Body: createCmd(0).Body}); err != nil {
		t.Fatal(err)
	}
	w.FailAppend = errors.New("broker down")
	_, err := r.Submit(ctx, &enginev1.Command{Body: submitCmd(0, "a", 40).Body})
	if !errors.Is(err, ErrWALProduce) {
		t.Fatalf("expected ErrWALProduce, got %v", err)
	}
	fr.mu.Lock()
	defer fr.mu.Unlock()
	if len(fr.calls) != 1 {
		t.Fatalf("fatal hook calls: %v", fr.calls)
	}
	if r.eng.LastSeq() != 1 {
		t.Fatal("failed batch must not be applied")
	}
	if w.Appends != 1+1+walRetries {
		t.Fatalf("appends=%d, want the first try plus %d retries", w.Appends, walRetries)
	}
	// The failed batch's seqs are reused: the next command is seq 2 in the
	// response and in the journal, and a replay of the journal yields the
	// same book as the live engine.
	w.FailAppend = nil
	res, err := r.Submit(ctx, &enginev1.Command{Body: submitCmd(0, "b", 41).Body})
	if err != nil || res.Seq != 2 {
		t.Fatalf("after recovery: res=%+v err=%v", res, err)
	}
	var stored enginev1.Command
	if len(w.Records[0]) != 2 || protoUnmarshal(w.Records[0][1], &stored) != nil || stored.Seq != 2 {
		t.Fatalf("journal: records=%d stored seq=%d", len(w.Records[0]), stored.Seq)
	}
	fresh := engine.New(0)
	if n, err := ReplayInto(ctx, w, 0, fresh, slog.Default()); err != nil || n != 2 {
		t.Fatalf("replay: n=%d err=%v", n, err)
	}
	liveLevels, liveSeq, liveStatus, _ := r.Book("mkt_demo")
	gotLevels, gotSeq, gotStatus, ok := fresh.Book("mkt_demo")
	if !ok || gotSeq != liveSeq || gotStatus != liveStatus || len(gotLevels) != len(liveLevels) || len(gotLevels) != 1 {
		t.Fatalf("replayed book differs: seq %d vs %d, levels %d vs %d", gotSeq, liveSeq, len(gotLevels), len(liveLevels))
	}
	for i := range gotLevels {
		if !proto.Equal(gotLevels[i], liveLevels[i]) {
			t.Fatalf("level %d differs: %v vs %v", i, gotLevels[i], liveLevels[i])
		}
	}
}

func TestRunnerProduceFailureFailsEveryWaiter(t *testing.T) {
	r, w, fr, _ := newRunnerT(t)
	ctx := context.Background()
	// Stall the loop in a publish retry so several requests queue behind it.
	w.SetFailPublish(errors.New("events down"))
	done := make(chan error, 1)
	go func() {
		_, err := r.Submit(ctx, &enginev1.Command{Body: createCmd(0).Body})
		done <- err
	}()
	waitFor(t, "a publish failure", func() bool { return testutil.ToFloat64(r.m.PublishFailures) >= 1 })
	const n = 5
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		go func(i int) {
			_, err := r.Submit(ctx, &enginev1.Command{Body: submitCmd(0, "o"+itoa(i), 40).Body})
			errs <- err
		}(i)
	}
	waitFor(t, "the queue to fill", func() bool { return len(r.reqs) == n })
	w.FailAppend = errors.New("broker down")
	w.SetFailPublish(nil)
	if err := <-done; err != nil {
		t.Fatalf("stalled batch must still complete: %v", err)
	}
	for i := 0; i < n; i++ {
		if err := <-errs; !errors.Is(err, ErrWALProduce) {
			t.Fatalf("waiter %d: expected ErrWALProduce, got %v", i, err)
		}
	}
	if fr.count() != 1 || r.eng.LastSeq() != 1 || len(w.Records[0]) != 1 {
		t.Fatalf("fatal=%d last=%d wal=%d", fr.count(), r.eng.LastSeq(), len(w.Records[0]))
	}
}

func TestRunnerShutdownDuringProduceRetryIsNotFatal(t *testing.T) {
	r, w, fr, cancel := newRunnerT(t)
	r.walBackoff = backoff{base: time.Hour, max: time.Hour}
	ctx := context.Background()
	w.FailAppend = errors.New("broker down")
	done := make(chan error, 1)
	go func() {
		_, err := r.Submit(ctx, &enginev1.Command{Body: createCmd(0).Body})
		done <- err
	}()
	waitFor(t, "the first append failure", func() bool { return w.AppendCount() == 1 })
	cancel()
	if err := <-done; !errors.Is(err, ErrShuttingDown) {
		t.Fatalf("expected ErrShuttingDown, got %v", err)
	}
	<-r.Done()
	if fr.count() != 0 {
		t.Fatalf("shutdown must not call Fatal: %v", fr.calls)
	}
}

func TestRunnerRetriesEventPublishUntilItSucceeds(t *testing.T) {
	r, w, fr, _ := newRunnerT(t)
	ctx := context.Background()
	if _, err := r.Submit(ctx, &enginev1.Command{Body: createCmd(0).Body}); err != nil {
		t.Fatal(err)
	}
	w.SetFailPublish(errors.New("events down"))
	done := make(chan error, 1)
	go func() {
		_, err := r.Submit(ctx, &enginev1.Command{Body: submitCmd(0, "a", 40).Body})
		done <- err
	}()
	waitFor(t, "three publish failures", func() bool { return testutil.ToFloat64(r.m.PublishFailures) >= 3 })
	select {
	case err := <-done:
		t.Fatalf("request completed while its events were unpublished: err=%v", err)
	default:
	}
	// A reader concurrent with the blocked partition still sees the applied state.
	if _, seq, _, ok := r.Book("mkt_demo"); !ok || seq != 2 {
		t.Fatalf("book during publish retry: ok=%v seq=%d", ok, seq)
	}
	w.SetFailPublish(nil)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if w.EventCount() != 3 || fr.count() != 0 {
		t.Fatalf("events=%d fatal=%d", w.EventCount(), fr.count())
	}
}

// After cancel the runner keeps retrying for shutdownGrace, then fails the
// waiters with ErrShuttingDown.
func TestRunnerShutdownDuringPublishRetryFailsPending(t *testing.T) {
	r, w, fr, cancel := newRunnerT(t)
	r.shutdownGrace = 5 * time.Millisecond
	ctx := context.Background()
	w.SetFailPublish(errors.New("events down"))
	done := make(chan error, 1)
	go func() {
		_, err := r.Submit(ctx, &enginev1.Command{Body: createCmd(0).Body})
		done <- err
	}()
	waitFor(t, "a publish failure", func() bool { return testutil.ToFloat64(r.m.PublishFailures) >= 1 })
	before := w.PublishCount()
	cancel()
	if err := <-done; !errors.Is(err, ErrShuttingDown) {
		t.Fatalf("expected ErrShuttingDown, got %v", err)
	}
	<-r.Done()
	if w.PublishCount() <= before {
		t.Fatal("no publish attempt was made during the shutdown grace")
	}
	if fr.count() != 0 || len(w.Records[0]) != 1 || r.eng.LastSeq() != 1 || w.EventCount() != 0 {
		t.Fatalf("fatal=%d wal=%d last=%d events=%d", fr.count(), len(w.Records[0]), r.eng.LastSeq(), w.EventCount())
	}
}

// A publish that succeeds within the grace completes the waiters normally.
func TestRunnerPublishSucceedsWithinShutdownGrace(t *testing.T) {
	r, w, fr, cancel := newRunnerT(t)
	r.shutdownGrace = 5 * time.Second
	ctx := context.Background()
	w.SetFailPublish(errors.New("events down"))
	done := make(chan outcome, 1)
	go func() {
		res, err := r.Submit(ctx, &enginev1.Command{Body: createCmd(0).Body})
		done <- outcome{res: res, err: err}
	}()
	waitFor(t, "a publish failure", func() bool { return testutil.ToFloat64(r.m.PublishFailures) >= 1 })
	cancel()
	after := testutil.ToFloat64(r.m.PublishFailures)
	waitFor(t, "retries to continue after cancel", func() bool { return testutil.ToFloat64(r.m.PublishFailures) >= after+2 })
	select {
	case o := <-done:
		t.Fatalf("completed during the grace without publishing: %+v", o)
	default:
	}
	w.SetFailPublish(nil)
	o := <-done
	if o.err != nil || o.res.Seq != 1 || len(o.res.Events) != 1 {
		t.Fatalf("expected a normal result, got %+v", o)
	}
	<-r.Done()
	if w.EventCount() != 1 || fr.count() != 0 {
		t.Fatalf("events=%d fatal=%d", w.EventCount(), fr.count())
	}
}

func TestRunnerShutdownFailsPendingWithUnavailable(t *testing.T) {
	r, _, _, cancel := newRunnerT(t)
	cancel()
	<-r.Done()
	ctx, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	_, err := r.Submit(ctx, &enginev1.Command{Body: createCmd(0).Body})
	if !errors.Is(err, ErrShuttingDown) {
		t.Fatalf("expected ErrShuttingDown, got %v", err)
	}
}

func TestRunnerSubmitRespectsCallerContext(t *testing.T) {
	r, _, _, _ := newRunnerT(t)
	ctx, stop := context.WithCancel(context.Background())
	stop()
	if _, err := r.Submit(ctx, &enginev1.Command{Body: createCmd(0).Body}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled caller context must return context.Canceled, got %v", err)
	}
}
