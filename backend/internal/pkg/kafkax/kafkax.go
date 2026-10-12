// Package kafkax holds franz-go client construction, idempotent topic
// creation and protobuf record helpers shared by all Kafka users.
package kafkax

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

// Topic names and retention from the M1 contracts.
const (
	TopicEngineWAL    = "engine.wal"
	TopicEngineEvents = "engine.events"
	SevenDaysMs       = int64(7 * 24 * time.Hour / time.Millisecond)
)

// TopicSpec describes a topic EnsureTopics creates. RetentionMs -1 = forever.
type TopicSpec struct {
	Name        string
	Partitions  int32
	RetentionMs int64
}

// M1Topics returns the milestone 1 topics.
func M1Topics() []TopicSpec {
	return []TopicSpec{
		{Name: TopicEngineWAL, Partitions: 1, RetentionMs: SevenDaysMs},
		{Name: TopicEngineEvents, Partitions: 1, RetentionMs: SevenDaysMs},
	}
}

// CleanBrokers trims entries and drops empties; an empty result is an error.
func CleanBrokers(brokers []string) ([]string, error) {
	out := make([]string, 0, len(brokers))
	for _, b := range brokers {
		if b = strings.TrimSpace(b); b != "" {
			out = append(out, b)
		}
	}
	if len(out) == 0 {
		return nil, errors.New("kafkax: no brokers configured (KAFKA_BROKERS)")
	}
	return out, nil
}

// NewClient builds a franz-go client seeded with brokers. It does not dial.
func NewClient(brokers []string, opts ...kgo.Opt) (*kgo.Client, error) {
	bs, err := CleanBrokers(brokers)
	if err != nil {
		return nil, err
	}
	cl, err := kgo.NewClient(append([]kgo.Opt{kgo.SeedBrokers(bs...)}, opts...)...)
	if err != nil {
		return nil, fmt.Errorf("kafkax: new client: %w", err)
	}
	return cl, nil
}

// NewProducer builds a client with acks=all and 1 ms linger. franz-go
// enables the idempotent producer by default with acks=all; never pass
// kgo.DisableIdempotentWrite. Record delivery is bounded at 10 s so a
// produce against an unreachable broker fails instead of hanging; callers
// may override it via opts (e.g. kgo.RecordDeliveryTimeout).
func NewProducer(brokers []string, opts ...kgo.Opt) (*kgo.Client, error) {
	base := []kgo.Opt{
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerLinger(time.Millisecond),
		kgo.RecordDeliveryTimeout(10 * time.Second),
	}
	return NewClient(brokers, append(base, opts...)...)
}

func validateSpecs(specs []TopicSpec) error {
	seen := map[string]bool{}
	for _, s := range specs {
		if s.Name == "" || s.Partitions < 1 || (s.RetentionMs != -1 && s.RetentionMs <= 0) || seen[s.Name] {
			return fmt.Errorf("kafkax: invalid topic spec %+v", s)
		}
		seen[s.Name] = true
	}
	return nil
}

// EnsureTopics creates missing topics; existing topics are left unchanged
// (TopicAlreadyExists is success), so it is safe to call on every start.
func EnsureTopics(ctx context.Context, brokers []string, specs []TopicSpec) error {
	if err := validateSpecs(specs); err != nil {
		return err
	}
	cl, err := NewClient(brokers)
	if err != nil {
		return err
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	var errs []error
	for _, s := range specs {
		retention := strconv.FormatInt(s.RetentionMs, 10)
		// kadm.CreateTopic returns the per-topic error (resp.Err) as err, so
		// TopicAlreadyExists must be checked on err itself.
		_, err := adm.CreateTopic(ctx, s.Partitions, -1, map[string]*string{"retention.ms": &retention}, s.Name)
		if err != nil && !errors.Is(err, kerr.TopicAlreadyExists) {
			errs = append(errs, fmt.Errorf("kafkax: create %s: %w", s.Name, err))
		}
	}
	return errors.Join(errs...)
}

// Record marshals msg into a record for topic with the given key.
func Record(topic, key string, msg proto.Message) (*kgo.Record, error) {
	b, err := proto.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("kafkax: marshal %T: %w", msg, err)
	}
	return &kgo.Record{Topic: topic, Key: []byte(key), Value: b}, nil
}

// Decode unmarshals rec.Value into msg; errors name topic/partition@offset.
func Decode(rec *kgo.Record, msg proto.Message) error {
	if err := proto.Unmarshal(rec.Value, msg); err != nil {
		return fmt.Errorf("kafkax: decode %s/%d@%d: %w", rec.Topic, rec.Partition, rec.Offset, err)
	}
	return nil
}

// PartitionKey is the engine.wal record key: the partition as decimal.
func PartitionKey(p uint32) string { return strconv.FormatUint(uint64(p), 10) }
