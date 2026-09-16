package ws

type Client struct {
	clientId string
	dial     string
}

func NewClient(clientId string, dial string) *Client {
	return &Client{
		clientId: clientId,
		dial:     dial,
	}
}

func (c *Client) CreateConnection()
