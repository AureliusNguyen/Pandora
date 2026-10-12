package matcher

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"google.golang.org/protobuf/proto"

	enginev1 "github.com/aureliusnguyen/pandora/backend/gen/go/pandora/engine/v1"
	"github.com/aureliusnguyen/pandora/backend/internal/engine"
)

// ErrGap means the WAL skipped a seq; the engine state cannot be trusted
// and the process must exit with code 3 (main does that).
var ErrGap = errors.New("matcher: wal seq gap")

// ReplayInto feeds every Command on partition (offset 0 to the high
// watermark) into eng. Repeated seqs are skipped, a gap returns ErrGap.
// Events are not published: replay only rebuilds state.
func ReplayInto(ctx context.Context, wal WAL, partition uint32, eng *engine.Engine, log *slog.Logger) (applied int, err error) {
	err = wal.Replay(ctx, partition, func(offset int64, value []byte) error {
		var cmd enginev1.Command
		if err := proto.Unmarshal(value, &cmd); err != nil {
			return fmt.Errorf("wal offset %d: unmarshal: %w", offset, err)
		}
		last := eng.LastSeq()
		switch {
		case cmd.Seq <= last:
			log.Warn("replay: repeated seq skipped", "seq", cmd.Seq, "offset", offset)
			return nil
		case cmd.Seq != last+1:
			return fmt.Errorf("%w: offset %d has seq %d, expected %d", ErrGap, offset, cmd.Seq, last+1)
		}
		eng.Apply(&cmd)
		applied++
		return nil
	})
	return applied, err
}
