package redispubsub

import (
	"context"
	"fmt"
	"log"
	"time"

	"main/app"

	"github.com/redis/go-redis/v9"
)

type Config struct {
	Addr           string
	Username       string
	Password       string
	DB             int
	ChannelPrefix  string
	PublishTimeout time.Duration
}

func ChannelName(prefix, instanceName string) string {
	return prefix + instanceName
}

type Publisher struct {
	rdb     redis.UniversalClient
	prefix  string
	timeout time.Duration
}

var _ app.MessagePublisher = (*Publisher)(nil)

func NewMessagePublisher(ctx context.Context, c Config) (*Publisher, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     c.Addr,
		Username: c.Username,
		Password: c.Password,
		DB:       c.DB,
	})

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := rdb.Ping(pingCtx).Err(); err != nil {
		_ = rdb.Close()
		return nil, fmt.Errorf("redis ping: %w", err)
	}

	timeout := c.PublishTimeout
	if timeout == 0 {
		timeout = 5 * time.Second
	}

	return &Publisher{rdb: rdb, prefix: c.ChannelPrefix, timeout: timeout}, nil
}

func (p *Publisher) PublishMessage(ctx context.Context, m app.Message, instancesNames []string) error {
	if len(instancesNames) == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	pipe := p.rdb.Pipeline()
	for _, name := range instancesNames {
		pipe.Publish(ctx, ChannelName(p.prefix, name), m.Value)
	}

	cmds, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("redis publish: %w", err)
	}
	for i, cmd := range cmds {
		if ic, ok := cmd.(*redis.IntCmd); ok && ic.Val() == 0 {
			log.Printf("redis: no subscribers on %q, message dropped", ChannelName(p.prefix, instancesNames[i]))
		}
	}

	return nil
}

func (p *Publisher) Close() error {
	return p.rdb.Close()
}
