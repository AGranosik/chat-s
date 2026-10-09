package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type instanceMessage struct {
	Key, Value   []byte
	Headers      map[string]string
	Timestamp    time.Time
	InstanceName string
}

type Connection struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	mu      sync.Mutex
	cfg     Config
}

type Config struct {
	Dial, Exchange, Kind string
	Mandatory            bool
}

func NewConnection(c Config) (*Connection, error) {
	conn, err := amqp.Dial(c.Dial)
	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()

	if err != nil {
		return nil, err
	}

	if err := ch.ExchangeDeclare(c.Exchange, c.Kind, true, false, false, false, nil); err != nil {
		return nil, err
	}

	return &Connection{
		conn:    conn,
		channel: ch,
		cfg:     c,
	}, nil
}

func (c *Connection) PublishMessage(ctx context.Context, m instanceMessage) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.ensureChannel(); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	//todo: make sure room id is in message
	roomId := string(m.Key)
	fmt.Println("Msg for %s - received")
	conf, err := c.channel.PublishWithDeferredConfirmWithContext(ctx,
		c.cfg.Exchange,
		roomId, //routing key
		c.cfg.Mandatory,
		false,
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			Timestamp:    m.Timestamp,
			Body:         m.Value,
		})

	if err != nil {
		return err
	}

	acked, err := conf.WaitContext(ctx)
	if err != nil { // unknown state, don't reuse
		return fmt.Errorf("rabbitmq confirm: %w", err)
	}
	if !acked {
		return errors.New("rabbitmq: message nacked by broker")
	}
	return nil
}
func (c *Connection) ensureChannel() error {
	if c.conn == nil || c.conn.IsClosed() {
		conn, err := amqp.Dial(c.cfg.Dial)
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
		if err := ch.ExchangeDeclare(c.cfg.Exchange, c.cfg.Kind, true, false, false, false, nil); err != nil {
			_ = ch.Close()
			return err
		}
		c.channel = ch
	}
	return nil
}

func (c *Connection) dropChannel() {
	if c.channel != nil {
		_ = c.channel.Close()
		c.channel = nil
	}
}

// TODO: close conn in main
func (c *Connection) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.dropChannel()
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}
