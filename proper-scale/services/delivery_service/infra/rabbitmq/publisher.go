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

// TODO: OUTBOX??
func (p publisher) PublishMessage(ctx context.Context, m app.Message, instancesNames []string) error {
	for _, v := range instancesNames {
		err := p.c.PublishMessage(ctx, instanceMessage{
			Key:          m.Key,
			Value:        m.Value,
			Headers:      m.Headers,
			Timestamp:    m.Timestamp,
			InstanceName: v,
		})
		if err != nil {
			return err
		}
	}

	return nil
}
