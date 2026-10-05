package rabbitmq

import (
	"context"
	"main/app"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Connection struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	mu      sync.Mutex
	cfg     Config
}

type Config struct {
	dial, exchange, routingKey, kind string
	mandatory, imediate              bool
}

func NewConnection(c Config) (*Connection, error) {
	conn, err := amqp.Dial(c.dial)
	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()

	if err != nil {
		return nil, err
	}

	if err := ch.ExchangeDeclare(c.exchange, c.kind, true, false, false, false, nil); err != nil {
		return nil, err
	}

	return &Connection{
		conn:    conn,
		channel: ch,
	}, nil
}

func (c *Connection) PublishMessage(ctx context.Context, m app.Message) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.ensureChannel(); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	//todo: make sure room id is in message
	err := c.channel.PublishWithContext(ctx,
		c.cfg.exchange,
		c.cfg.routingKey,
		c.cfg.mandatory,
		c.cfg.imediate,
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			Timestamp:    time.Now(),
			Body:         m.Value,
		})

	if err != nil {
		return err
	}
	return nil
}

func (c *Connection) ensureChannel() error {
	if c.conn == nil || c.conn.IsClosed() {
		conn, err := amqp.Dial(c.cfg.dial)
		if err != nil {
			return err
		}
		c.conn = conn
	}
	if c.channel == nil || c.channel.IsClosed() {
		ch, err := c.conn.Channel()
		if err != nil {
			return err
		}
		if err := ch.Confirm(false); err != nil {
			return err
		}
		c.channel = ch
	}
	return nil
}
