package redis

import (
	"context"
	"errors"
	"fmt"
	"log"
	"messages/chat"
	"time"

	"github.com/redis/go-redis/v9"
)

type ConsumerConfig struct {
	ChannelPrefix string
	InstanceName  string
	BufferSize    int
}

type Consumer struct {
	rdb     redis.UniversalClient
	channel string
	bufSize int
	handler chat.PayloadHandler
}

func NewConsumer(rdb redis.UniversalClient, c ConsumerConfig, h chat.PayloadHandler) (*Consumer, error) {
	if c.InstanceName == "" {
		return nil, errors.New("instance name is required")
	}
	if h == nil {
		return nil, errors.New("handler is required")
	}
	return &Consumer{
		rdb:     rdb,
		channel: channelName(c.ChannelPrefix, c.InstanceName),
		bufSize: c.BufferSize,
		handler: h,
	}, nil
}

func (c *Consumer) Run(ctx context.Context) error {
	sub := c.rdb.Subscribe(ctx, c.channel)
	defer sub.Close()

	confirmCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	_, err := sub.Receive(confirmCtx)
	cancel()
	if err != nil {
		return fmt.Errorf("subscribe %q: %w", c.channel, err)
	}

	var opts []redis.ChannelOption
	if c.bufSize > 0 {
		opts = append(opts, redis.WithChannelSize(c.bufSize))
	}
	msgs := sub.Channel(opts...)

	log.Printf("redis: subscribed to %q", c.channel)

	for {
		select {
		case <-ctx.Done():
			return nil
		case msg, ok := <-msgs:
			if !ok {
				return fmt.Errorf("subscription to %q closed", c.channel)
			}
			if err := c.handler.HandlePayload(ctx, []byte(msg.Payload)); err != nil {
				log.Printf("redis: handle message on %q: %v", c.channel, err)
			}
		}
	}
}

// TODO: to common
func channelName(prefix, instanceName string) string {
	return prefix + instanceName
}
