package main

import (
	"encoding/json"
	"log"
	"net/url"
	"os"
	"os/signal"
	"time"

	"github.com/gorilla/websocket"
)

type Message struct {
	RoomID  string          `json:"room_id"`
	Payload json.RawMessage `json:"payload"`
}

func main() {
	u := url.URL{Scheme: "ws", Host: "localhost:80", Path: "/ws"}

	q := u.Query()
	q.Set("clientId", "abc123")
	u.RawQuery = q.Encode()
	log.Printf("connecting to %s", u.String())

	c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		log.Fatal("dial:", err)
	}
	defer c.Close()

	done := make(chan struct{})

	// Reader goroutine
	go func() {
		defer close(done)
		for {
			_, message, err := c.ReadMessage()
			if err != nil {
				log.Println("read:", err)
				return
			}
			log.Printf("recv: %s", message)
		}
	}()

	payload := "test msg."
	serialized, _ := json.Marshal(payload)
	msg, err := json.Marshal(Message{
		RoomID:  "2",
		Payload: serialized,
	})

	log.Printf("Sending a msg.")
	// Send a test message
	err = c.WriteMessage(websocket.TextMessage, msg)
	if err != nil {
		log.Println("write:", err)
		return
	}

	// Keep alive until interrupted
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	select {
	case <-done:
	case <-interrupt:
		c.WriteMessage(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
		time.Sleep(time.Second)
	}
}
