package rabbitmq

import (
	"context"
	"main/app"
)

type publisher struct {
	c *Connection
}

func NewMessagePublisher(c Config) (app.MessagePublisher, error) {
	conn, err := NewConnection(c)
	if err != nil {
		return nil, err
	}

	return publisher{
		c: conn,
	}, nil
}

func (p publisher) PublishMessage(ctx context.Context, m app.Message) error {
	// some logic before propgation
	return p.c.PublishMessage(ctx, m)
}
