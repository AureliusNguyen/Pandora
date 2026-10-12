package matcher

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/aureliusnguyen/pandora/backend/gen/go/pandora/common/v1"
	enginev1 "github.com/aureliusnguyen/pandora/backend/gen/go/pandora/engine/v1"
	"github.com/aureliusnguyen/pandora/backend/internal/engine"
)

var (
	// ErrShuttingDown is returned to requests that cannot be journaled
	// because the runner stopped. Handlers map it to CodeUnavailable.
	ErrShuttingDown = errors.New("matcher: shutting down")
	// ErrWALProduce wraps a WAL batch that still failed after walRetries
	// retries. The runner then calls Fatal: the broker may hold part of the
	// batch, so the only safe recovery is a restart that replays the journal.
	// Worst case time to Fatal against a dead broker: six attempts bounded
	// by KafkaWAL's 10 s RecordDeliveryTimeout plus 5 s of backoff, ~65 s.
	ErrWALProduce = errors.New("matcher: wal produce failed")
	// ErrMarshal wraps a command that could not be marshaled; nothing was
	// journaled. Handlers map it to CodeInternal.
	ErrMarshal = errors.New("matcher: marshal command")
)

// walRetries is how many times a failed WAL append is retried before the
// runner gives up and calls Fatal; with walBackoffDefault the retries span
// about five seconds.
const walRetries = 5

// backoff is an exponential retry delay: base, doubling, capped at max.
type backoff struct {
	base, max time.Duration
}

var (
	// 200 ms, 400 ms, 800 ms, 1.6 s, 2 s: five retries over ~5 s.
	walBackoffDefault = backoff{base: 200 * time.Millisecond, max: 2 * time.Second}
	// 50 ms doubling to 2 s, forever (ruling X3).
	publishBackoffDefault = backoff{base: 50 * time.Millisecond, max: 2 * time.Second}
)

// next returns the delay for attempt (0-based).
func (b backoff) next(attempt int) time.Duration {
	d := b.base
	for i := 0; i < attempt && d < b.max; i++ {
		d *= 2
	}
	return min(d, b.max)
}

// Result is what a journaled and applied command returns to its caller.
type Result struct {
	Seq        uint64
	EngineTime int64
	Events     []*enginev1.Event
}

type outcome struct {
	res Result
	err error
}

type request struct {
	cmd  *enginev1.Command
	done chan outcome // buffered 1
}

// Runner owns one partition's engine. One goroutine sequences, journals,
// applies and publishes; everything else talks to it through Submit.
type Runner struct {
	partition uint32
	partLabel string
	eng       *engine.Engine
	wal       WAL
	m         *Metrics
	log       *slog.Logger
	tracer    trace.Tracer
	// reqs is the Submit queue (4*MaxBatch deep). Backpressure: when the
	// loop is blocked (a WAL retry or a publish retry during a broker
	// outage) the channel fills and Submit blocks on the send until the
	// caller's context ends, so the caller sees Canceled/DeadlineExceeded
	// (the gateway maps those to a retryable status) and nothing is
	// journaled for it.
	reqs           chan *request
	stopped        chan struct{}
	mu             sync.Mutex // guards eng for Book/HasMarket readers
	walBackoff     backoff
	publishBackoff backoff
	// shutdownGrace bounds how long a publish keeps retrying after the
	// runner ctx is canceled (Options.ShutdownGrace, default closeFlushTimeout).
	shutdownGrace time.Duration

	// Now stamps engine_time_unix_nanos (unix nanos). Replaceable in tests.
	Now func() int64
	// Fatal is called when the WAL cannot be trusted. Default logs and exits 3.
	Fatal func(msg string, err error)
}

// NewRunner wires a runner for partition; call Start before Submit.
func NewRunner(partition uint32, eng *engine.Engine, wal WAL, m *Metrics, log *slog.Logger) *Runner {
	r := &Runner{
		partition:      partition,
		partLabel:      strconv.FormatUint(uint64(partition), 10),
		eng:            eng,
		wal:            wal,
		m:              m,
		log:            log.With("partition", partition),
		tracer:         otel.Tracer("pandora/matcher"),
		reqs:           make(chan *request, 4*MaxBatch),
		stopped:        make(chan struct{}),
		walBackoff:     walBackoffDefault,
		publishBackoff: publishBackoffDefault,
		shutdownGrace:  closeFlushTimeout,
		Now:            func() int64 { return time.Now().UnixNano() },
	}
	r.Fatal = func(msg string, err error) {
		r.log.Error(msg, "err", err)
		os.Exit(3)
	}
	return r
}

// Start launches the partition goroutine; it exits when ctx is done. The
// caller derives ctx from context.WithoutCancel so that only an explicit
// Stop ends the loop, never a request's context.
func (r *Runner) Start(ctx context.Context) { go r.loop(ctx) }

// Done is closed once the loop has exited and all pending requests failed.
func (r *Runner) Done() <-chan struct{} { return r.stopped }

// Submit enqueues cmd (body only; seq/epoch/time are stamped here) and
// waits for it to be journaled, applied and its events published.
func (r *Runner) Submit(ctx context.Context, cmd *enginev1.Command) (Result, error) {
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	q := &request{cmd: cmd, done: make(chan outcome, 1)}
	select {
	case r.reqs <- q:
	case <-r.stopped:
		return Result{}, ErrShuttingDown
	case <-ctx.Done():
		return Result{}, ctx.Err()
	}
	select {
	case o := <-q.done:
		return o.res, o.err
	case <-ctx.Done():
		// The caller gave up; the command is still journaled and applied
		// (the client retries with the same client_order_id and is told
		// DUPLICATE_CLIENT_ORDER_ID), only this response is lost.
		return Result{}, ctx.Err()
	case <-r.stopped:
		// The loop may have completed it just before stopping.
		select {
		case o := <-q.done:
			return o.res, o.err
		default:
			return Result{}, ErrShuttingDown
		}
	}
}

// Book snapshots a market from the engine (serialized against apply).
func (r *Runner) Book(marketID string) ([]*commonv1.Level, uint64, commonv1.MarketStatus, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.eng.Book(marketID)
}

// HasMarket reports whether the engine knows marketID.
func (r *Runner) HasMarket(marketID string) bool {
	_, ok := r.Outcomes(marketID)
	return ok
}

// Outcomes returns marketID's outcome count (ok false when unknown).
func (r *Runner) Outcomes(marketID string) (uint32, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.eng.Outcomes(marketID)
}

func (r *Runner) loop(ctx context.Context) {
	defer close(r.stopped)
	batch := make([]*request, 0, MaxBatch)
	values := make([][]byte, 0, MaxBatch)
	for {
		var first *request
		select {
		case <-ctx.Done():
			r.drainFail()
			return
		case first = <-r.reqs:
		}
		batch = append(batch[:0], first)
	drain:
		for len(batch) < MaxBatch {
			select {
			case q := <-r.reqs:
				batch = append(batch, q)
			default:
				break drain
			}
		}
		r.flush(ctx, batch, values[:0])
	}
}

// drainFail answers every queued request with ErrShuttingDown. Requests
// that race the close are caught by Submit's stopped check.
func (r *Runner) drainFail() {
	for {
		select {
		case q := <-r.reqs:
			q.done <- outcome{err: ErrShuttingDown}
		default:
			return
		}
	}
}

func failAll(batch []*request, err error) {
	for _, q := range batch {
		q.done <- outcome{err: err}
	}
}

// flush stamps, journals, applies and publishes one batch, then completes
// its requests. A request is acked only after its command is journaled,
// applied and its events are published.
func (r *Runner) flush(ctx context.Context, batch []*request, values [][]byte) {
	r.m.BatchSize.Observe(float64(len(batch)))
	base := r.eng.LastSeq()
	for i, q := range batch {
		q.cmd.Seq = base + uint64(i) + 1
		q.cmd.Partition = r.partition
		q.cmd.Epoch = Epoch
		q.cmd.EngineTimeUnixNanos = r.Now()
		b, err := proto.Marshal(q.cmd)
		if err != nil {
			// Only a malformed proto can do this; nothing was journaled.
			failAll(batch, fmt.Errorf("%w: %w", ErrMarshal, err))
			return
		}
		values = append(values, b)
	}

	if err := r.appendWithRetry(ctx, len(batch), values); err != nil {
		if errors.Is(err, ErrShuttingDown) {
			failAll(batch, err)
			return
		}
		// Fatal runs before the requests complete so a test hook observes
		// it deterministically; the default hook exits 3 right here.
		r.Fatal("wal produce failed; exiting so replay re-establishes the sequence", err)
		failAll(batch, fmt.Errorf("%w: %w", ErrWALProduce, err))
		return
	}

	_, span := r.tracer.Start(ctx, "matcher.engine.apply", trace.WithAttributes(attribute.Int("batch.size", len(batch))))
	t0 := time.Now()
	results := make([]Result, len(batch))
	var events []*enginev1.Event
	for i, q := range batch {
		r.mu.Lock()
		evs := r.eng.Apply(q.cmd)
		r.mu.Unlock()
		r.m.Commands.WithLabelValues(commandType(q.cmd)).Inc()
		results[i] = Result{Seq: q.cmd.Seq, EngineTime: q.cmd.EngineTimeUnixNanos, Events: evs}
		events = append(events, evs...)
	}
	r.m.Apply.Observe(time.Since(t0).Seconds())
	r.observeLastSeq()
	span.End()

	if err := r.publishWithRetry(ctx, events); err != nil {
		// Only ErrShuttingDown: the commands are journaled and applied, but
		// their events never reached engine.events, so the callers are told
		// to retry (and learn DUPLICATE_CLIENT_ORDER_ID from the replayed state).
		failAll(batch, err)
		return
	}
	for i, q := range batch {
		q.done <- outcome{res: results[i]}
	}
}

// observeLastSeq sets the last_seq{partition} gauge from the engine.
func (r *Runner) observeLastSeq() {
	r.m.LastSeq.WithLabelValues(r.partLabel).Set(float64(r.eng.LastSeq()))
}

// sleep waits d or until ctx is done; it reports whether the wait completed.
func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// appendWithRetry journals values, retrying walRetries times. The produce
// itself is never canceled (an interrupted batch is ambiguous); when ctx
// ends between attempts the batch fails with ErrShuttingDown, which is a
// clean exit, not a WAL failure. Against a dead broker each attempt takes
// up to KafkaWAL's 10 s RecordDeliveryTimeout, so the caller sees the
// error (and Fatal runs) after about 6*10 s + 5 s of backoff = ~65 s;
// against a reachable broker that rejects the batch it is ~5 s.
func (r *Runner) appendWithRetry(ctx context.Context, n int, values [][]byte) error {
	pctx, span := r.tracer.Start(ctx, "matcher.wal.produce", trace.WithAttributes(attribute.Int("batch.size", n)))
	defer span.End()
	pctx = context.WithoutCancel(pctx)
	for attempt := 0; ; attempt++ {
		t0 := time.Now()
		err := r.wal.Append(pctx, r.partition, values)
		r.m.WALProduce.Observe(time.Since(t0).Seconds())
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ErrShuttingDown
		}
		if attempt == walRetries {
			return err
		}
		d := r.walBackoff.next(attempt)
		r.log.Error("wal produce failed; retrying", "err", err, "attempt", attempt+1, "retry_in", d)
		if !sleep(ctx, d) {
			return ErrShuttingDown
		}
	}
}

// publishWithRetry publishes events, retrying forever with exponential
// backoff (ruling X3: events are derived and at-least-once, never
// dropped). When ctx ends it keeps retrying for shutdownGrace, then gives
// up with ErrShuttingDown and logs the unpublished seq range.
func (r *Runner) publishWithRetry(ctx context.Context, events []*enginev1.Event) error {
	if len(events) == 0 {
		return nil
	}
	keys := make([]string, 0, len(events))
	values := make([][]byte, 0, len(events))
	for _, ev := range events {
		b, err := proto.Marshal(ev)
		if err != nil {
			// Only a malformed proto can do this; the event cannot be
			// represented on the wire, so it is reported and skipped.
			r.log.Error("marshal event", "err", err, "seq", ev.Seq, "idx", ev.Idx)
			continue
		}
		keys = append(keys, ev.MarketId)
		values = append(values, b)
	}
	pctx, span := r.tracer.Start(ctx, "matcher.events.publish", trace.WithAttributes(attribute.Int("events", len(values))))
	defer span.End()
	pctx = context.WithoutCancel(pctx)
	first, last := events[0].Seq, events[len(events)-1].Seq
	attempt := 0
	for ; ; attempt++ {
		err := r.wal.Publish(pctx, r.partition, keys, values)
		if err == nil {
			return nil
		}
		r.m.PublishFailures.Inc()
		d := r.publishBackoff.next(attempt)
		r.log.Error("publish events failed; retrying", "err", err, "attempt", attempt+1, "retry_in", d, "first_seq", first, "last_seq", last)
		if !sleep(ctx, d) {
			break
		}
	}
	// Shutdown began while the batch is unpublished: keep trying for the
	// grace period so a rollout does not lose the last events.
	gctx, cancel := context.WithTimeout(pctx, r.shutdownGrace)
	defer cancel()
	r.log.Warn("shutdown with unpublished events; retrying for the grace period", "grace", r.shutdownGrace, "first_seq", first, "last_seq", last)
	for ; ; attempt++ {
		err := r.wal.Publish(gctx, r.partition, keys, values)
		if err == nil {
			return nil
		}
		r.m.PublishFailures.Inc()
		d := r.publishBackoff.next(attempt)
		r.log.Error("publish events failed; retrying", "err", err, "attempt", attempt+1, "retry_in", d, "first_seq", first, "last_seq", last)
		if gctx.Err() != nil || !sleep(gctx, d) {
			r.log.Error("shutdown grace expired with unpublished events; callers get ErrShuttingDown", "first_seq", first, "last_seq", last, "events", len(values))
			return ErrShuttingDown
		}
	}
}
