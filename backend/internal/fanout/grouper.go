package fanout

import enginev1 "github.com/aureliusnguyen/pandora/backend/gen/go/pandora/engine/v1"

// CommandGrouper buffers consecutive events of one (partition, seq) command.
// A command is released only when the next command starts or Flush is called,
// so a command split across Kafka fetches still yields one delta. Grouping
// only batches messages: if an idle Flush splits a command, the BookView
// still applies the tail (see BookView.Apply).
type CommandGrouper struct {
	cur       []*enginev1.Event
	partition uint32
	seq       uint64
}

// Add buffers ev and returns the previous command's events when ev starts a
// new command, or nil while the current command is still open.
func (g *CommandGrouper) Add(ev *enginev1.Event) []*enginev1.Event {
	var out []*enginev1.Event
	if len(g.cur) > 0 && (ev.GetPartition() != g.partition || ev.GetSeq() != g.seq) {
		out = g.cur
		g.cur = nil
	}
	g.partition, g.seq = ev.GetPartition(), ev.GetSeq()
	g.cur = append(g.cur, ev)
	return out
}

// Flush returns and clears the held events (nil when none are held).
func (g *CommandGrouper) Flush() []*enginev1.Event {
	out := g.cur
	g.cur = nil
	return out
}

// Pending reports whether a command is being held.
func (g *CommandGrouper) Pending() bool { return len(g.cur) > 0 }
