package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	messagev1 "github.com/AGranosik/chat/contracts/messages"
	"github.com/gorilla/websocket"
)

const reconnectDelay = time.Second

type decodedFrame struct {
	kind     string
	accepted *messagev1.AcceptedAck
	rejected *messagev1.RejectedAck
	event    *messagev1.Event
}

func main() {
	endpoint := envOr("WS_URL", "ws://localhost:80/ws")
	clientID := envOr("WS_CLIENT_ID", "abc123")
	roomIDs := splitRooms(envOr("WS_ROOM_IDS", "1,2"))
	if len(roomIDs) == 0 {
		log.Fatal("WS_ROOM_IDS must contain at least one room")
	}
	sendRoomID := envOr("WS_SEND_ROOM_ID", "2")
	if !containsRoom(roomIDs, sendRoomID) {
		log.Fatalf("WS_SEND_ROOM_ID %q must be present in WS_ROOM_IDS", sendRoomID)
	}
	request, err := sampleSendRequest(sendRoomID)
	if err != nil {
		log.Fatal(err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := runClient(ctx, endpoint, clientID, roomIDs, request); err != nil {
		log.Fatal(err)
	}
}

func runClient(ctx context.Context, endpoint, clientID string, roomIDs []string, request messagev1.SendRequest) error {
	connectionURL, err := websocketURL(endpoint, clientID, roomIDs)
	if err != nil {
		return err
	}
	requestPending := true
	for ctx.Err() == nil {
		connection, _, err := websocket.DefaultDialer.DialContext(ctx, connectionURL, nil)
		if err != nil {
			log.Printf("websocket connection failed: %v", err)
			if !waitForReconnect(ctx) {
				break
			}
			continue
		}
		log.Printf("connected to %s", connectionURL)
		connectionDone := make(chan struct{})
		go func() {
			select {
			case <-ctx.Done():
				_ = connection.Close()
			case <-connectionDone:
			}
		}()

		if requestPending {
			if err := connection.WriteJSON(request); err != nil {
				log.Printf("send message: %v", err)
				close(connectionDone)
				_ = connection.Close()
				if !waitForReconnect(ctx) {
					break
				}
				continue
			}
			log.Printf("sent message with client_message_id %s", request.ClientMessageID)
		}

		for {
			_, data, err := connection.ReadMessage()
			if err != nil {
				log.Printf("websocket disconnected: %v", err)
				break
			}
			frame, err := decodeServerFrame(data)
			if err != nil {
				log.Printf("ignore invalid server frame: %v", err)
				continue
			}
			switch frame.kind {
			case messagev1.AcceptedFrameType:
				requestPending = false
				log.Printf("accepted message %s in room %s at sequence %d", frame.accepted.MessageID, frame.accepted.RoomID, frame.accepted.RoomSeq)
			case messagev1.RejectedFrameType:
				requestPending = false
				log.Printf("message %s rejected: %s", frame.rejected.ClientMessageID, frame.rejected.ErrorCode)
			case "event":
				log.Printf("msg received: room %s sequence %d: %s", frame.event.RoomID, frame.event.RoomSeq, frame.event.Payload)
			}
		}
		close(connectionDone)
		_ = connection.Close()
		if ctx.Err() == nil && !waitForReconnect(ctx) {
			break
		}
	}
	return nil
}

func decodeServerFrame(data []byte) (decodedFrame, error) {
	var header struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(data, &header); err != nil {
		return decodedFrame{}, fmt.Errorf("decode frame type: %w", err)
	}
	switch header.Type {
	case messagev1.AcceptedFrameType:
		var ack messagev1.AcceptedAck
		if err := json.Unmarshal(data, &ack); err != nil {
			return decodedFrame{}, err
		}
		if err := ack.Validate(); err != nil {
			return decodedFrame{}, err
		}
		return decodedFrame{kind: messagev1.AcceptedFrameType, accepted: &ack}, nil
	case messagev1.RejectedFrameType:
		var rejection messagev1.RejectedAck
		if err := json.Unmarshal(data, &rejection); err != nil {
			return decodedFrame{}, err
		}
		if err := rejection.Validate(); err != nil {
			return decodedFrame{}, err
		}
		return decodedFrame{kind: messagev1.RejectedFrameType, rejected: &rejection}, nil
	default:
		var event messagev1.Event
		if err := json.Unmarshal(data, &event); err != nil {
			return decodedFrame{}, err
		}
		if err := event.Validate(); err != nil {
			return decodedFrame{}, err
		}
		return decodedFrame{kind: "event", event: &event}, nil
	}
}

func websocketURL(endpoint, clientID string, roomIDs []string) (string, error) {
	parsed, err := url.Parse(endpoint)
	if err != nil {
		return "", fmt.Errorf("parse WebSocket URL: %w", err)
	}
	if (parsed.Scheme != "ws" && parsed.Scheme != "wss") || parsed.Host == "" {
		return "", fmt.Errorf("WebSocket URL must use ws or wss and include a host")
	}
	if strings.TrimSpace(clientID) == "" || len(roomIDs) == 0 {
		return "", fmt.Errorf("client ID and at least one room are required")
	}
	query := parsed.Query()
	query.Set("client_id", clientID)
	query.Set("room_ids", strings.Join(roomIDs, ","))
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}

func sampleSendRequest(roomID string) (messagev1.SendRequest, error) {
	var randomID [16]byte
	if _, err := rand.Read(randomID[:]); err != nil {
		return messagev1.SendRequest{}, fmt.Errorf("generate client message id: %w", err)
	}
	payload, err := json.Marshal(map[string]string{"text": "test msg."})
	if err != nil {
		return messagev1.SendRequest{}, err
	}
	return messagev1.SendRequest{
		Type:            messagev1.SendFrameType,
		Version:         messagev1.Version,
		ClientMessageID: hex.EncodeToString(randomID[:]),
		RoomID:          roomID,
		Payload:         payload,
	}, nil
}

func envOr(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

func splitRooms(value string) []string {
	parts := strings.Split(value, ",")
	rooms := make([]string, 0, len(parts))
	seen := make(map[string]struct{}, len(parts))
	for _, part := range parts {
		roomID := strings.TrimSpace(part)
		if roomID == "" {
			continue
		}
		if _, exists := seen[roomID]; exists {
			continue
		}
		seen[roomID] = struct{}{}
		rooms = append(rooms, roomID)
	}
	return rooms
}

func containsRoom(roomIDs []string, roomID string) bool {
	for _, candidate := range roomIDs {
		if candidate == roomID {
			return true
		}
	}
	return false
}

func waitForReconnect(ctx context.Context) bool {
	timer := time.NewTimer(reconnectDelay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}
