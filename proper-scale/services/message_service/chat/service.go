package chat

type Incoming struct {
	RoomId string
	data   []byte
}

type ChatService struct {
}

func NewChatService() *ChatService {
	return &ChatService{}
}

func (c *ChatService) HandleIncoming(roomId string, data []byte) error {
	//some validation there

	//add outbox
	return nil
}
