package kafka

import (
	"context"
	"errors"
	"testing"

	"github.com/IBM/sarama"
)

type fakeSyncProducer struct {
	sarama.SyncProducer
	calls int
	sent  []*sarama.ProducerMessage
	send  func([]*sarama.ProducerMessage) error
}

func (p *fakeSyncProducer) SendMessages(messages []*sarama.ProducerMessage) error {
	p.calls++
	p.sent = messages
	if p.send == nil {
		return nil
	}
	return p.send(messages)
}

func testMessages() []Message {
	return []Message{
		{Key: "room-1", Value: []byte(`{"seq":1}`)},
		{Key: "room-2", Value: []byte(`{"seq":1}`)},
		{Key: "room-1", Value: []byte(`{"seq":2}`)},
	}
}

func TestPublishBatchSendsAllMessagesInOneCall(t *testing.T) {
	fake := &fakeSyncProducer{}
	producer := &Producer{producer: fake}
	messages := testMessages()

	results := producer.PublishBatch(context.Background(), "messages", messages)

	if fake.calls != 1 {
		t.Fatalf("SendMessages calls = %d, want 1", fake.calls)
	}
	for i, err := range results {
		if err != nil {
			t.Fatalf("result %d = %v, want nil", i, err)
		}
	}
	for i, sent := range fake.sent {
		key, _ := sent.Key.Encode()
		value, _ := sent.Value.Encode()
		if sent.Topic != "messages" || string(key) != messages[i].Key || string(value) != string(messages[i].Value) {
			t.Fatalf("sent %d = %s/%s/%s, want messages/%s/%s", i, sent.Topic, key, value, messages[i].Key, messages[i].Value)
		}
	}
}

func TestPublishBatchMapsProducerErrorsToTheirMessages(t *testing.T) {
	brokerErr := errors.New("not enough replicas")
	fake := &fakeSyncProducer{send: func(sent []*sarama.ProducerMessage) error {
		return sarama.ProducerErrors{{Msg: sent[1], Err: brokerErr}}
	}}
	producer := &Producer{producer: fake}

	results := producer.PublishBatch(context.Background(), "messages", testMessages())

	if results[0] != nil || results[2] != nil {
		t.Fatalf("results = %v, want only index 1 to fail", results)
	}
	if !errors.Is(results[1], brokerErr) {
		t.Fatalf("result 1 = %v, want %v", results[1], brokerErr)
	}
}

func TestPublishBatchFailsEveryMessageOnUnattributedError(t *testing.T) {
	tests := []struct {
		name string
		send func([]*sarama.ProducerMessage) error
	}{
		{"plain error", func([]*sarama.ProducerMessage) error { return sarama.ErrClosedClient }},
		{"producer error for unknown message", func([]*sarama.ProducerMessage) error {
			return sarama.ProducerErrors{{Msg: &sarama.ProducerMessage{}, Err: sarama.ErrOutOfBrokers}}
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			producer := &Producer{producer: &fakeSyncProducer{send: tc.send}}

			results := producer.PublishBatch(context.Background(), "messages", testMessages())

			for i, err := range results {
				if err == nil {
					t.Fatalf("result %d = nil, want error", i)
				}
			}
		})
	}
}

func TestPublishBatchDoesNotSendWhenContextIsCancelled(t *testing.T) {
	fake := &fakeSyncProducer{}
	producer := &Producer{producer: fake}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	results := producer.PublishBatch(ctx, "messages", testMessages())

	if fake.calls != 0 {
		t.Fatalf("SendMessages calls = %d, want 0", fake.calls)
	}
	for i, err := range results {
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("result %d = %v, want context.Canceled", i, err)
		}
	}
}

func TestPublishBatchEmptyDoesNotSend(t *testing.T) {
	fake := &fakeSyncProducer{}
	producer := &Producer{producer: fake}

	if results := producer.PublishBatch(context.Background(), "messages", nil); len(results) != 0 {
		t.Fatalf("results = %v, want empty", results)
	}
	if fake.calls != 0 {
		t.Fatalf("SendMessages calls = %d, want 0", fake.calls)
	}
}
