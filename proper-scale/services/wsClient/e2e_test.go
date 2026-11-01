package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"testing"
	"time"

	messagev1 "github.com/AGranosik/chat/contracts/messages"
	"github.com/gorilla/websocket"
)

const oneOnOneTimeout = 20 * time.Second

// TestE2EOneOnOne exercises the live WebSocket -> outbox/Kafka -> Redis ->
// WebSocket path against an already-running stack.
func TestE2EOneOnOne(t *testing.T) {

	endpoint := envOr("WS_E2E_URL", "ws://localhost:80/ws")
	ctx, cancel := context.WithTimeout(context.Background(), oneOnOneTimeout)
	defer cancel()

	roomID := "e2e-" + randomE2EID(t)
	senderID := "sender-" + randomE2EID(t)
	receiverID := "receiver-" + randomE2EID(t)

	receiver := dialE2EClient(t, ctx, endpoint, receiverID, roomID)
	defer receiver.Close()
	sender := dialE2EClient(t, ctx, endpoint, senderID, roomID)
	defer sender.Close()

	text := "one-on-one-" + randomE2EID(t)
	request := messagev1.SendRequest{
		Type:            messagev1.SendFrameType,
		Version:         messagev1.Version,
		ClientMessageID: "client-message-" + randomE2EID(t),
		RoomID:          roomID,
		Payload:         json.RawMessage(fmt.Sprintf(`{"text":%q}`, text)),
	}
	if err := request.Validate(); err != nil {
		t.Fatalf("invalid test request: %v", err)
	}
	if err := sender.WriteJSON(request); err != nil {
		t.Fatalf("send message: %v", err)
	}

	ack := readAcceptedAck(t, sender, request.ClientMessageID)
	event := readRoomEvent(t, receiver, request.ClientMessageID)

	if ack.RoomID != roomID || event.RoomID != roomID {
		t.Fatalf("room IDs = ack %q, event %q; want %q", ack.RoomID, event.RoomID, roomID)
	}
	if event.MessageID != ack.MessageID || event.RoomSeq != ack.RoomSeq {
		t.Fatalf("delivered event ID/sequence = %q/%d; want acknowledgement %q/%d", event.MessageID, event.RoomSeq, ack.MessageID, ack.RoomSeq)
	}
	var gotPayload struct {
		Text string `json:"text"`
	}
	if err := json.Unmarshal(event.Payload, &gotPayload); err != nil {
		t.Fatalf("decode delivered payload %s: %v", event.Payload, err)
	}
	if gotPayload.Text != text {
		t.Fatalf("delivered payload text = %q, want %q", gotPayload.Text, text)
	}
}

func TestE2EMultipleClientsReceiveRoomMessage(t *testing.T) {
	const clientCount = 4
	endpoint := envOr("WS_E2E_URL", "ws://localhost:80/ws")
	ctx, cancel := context.WithTimeout(context.Background(), oneOnOneTimeout)
	defer cancel()

	roomID := "e2e-" + randomE2EID(t)
	clients := make([]*websocket.Conn, clientCount)
	for i := range clients {
		clientID := fmt.Sprintf("client-%d-%s", i, randomE2EID(t))
		clients[i] = dialE2EClient(t, ctx, endpoint, clientID, roomID)
		defer clients[i].Close()
	}

	text := "multi-client-" + randomE2EID(t)
	request := messagev1.SendRequest{
		Type:            messagev1.SendFrameType,
		Version:         messagev1.Version,
		ClientMessageID: "client-message-" + randomE2EID(t),
		RoomID:          roomID,
		Payload:         json.RawMessage(fmt.Sprintf(`{"text":%q}`, text)),
	}
	if err := request.Validate(); err != nil {
		t.Fatalf("invalid test request: %v", err)
	}
	if err := clients[0].WriteJSON(request); err != nil {
		t.Fatalf("sender write message: %v", err)
	}

	ack, senderEvent := readAcceptedAckAndEvent(t, clients[0], request.ClientMessageID)
	assertE2EEventMatches(t, senderEvent, ack, request, text)

	for i := 1; i < len(clients); i++ {
		event := readRoomEvent(t, clients[i], request.ClientMessageID)
		assertE2EEventMatches(t, event, ack, request, text)
	}
}

func TestE2ERoomSubscriptionsIsolateFanout(t *testing.T) {
	endpoint := envOr("WS_E2E_URL", "ws://localhost:80/ws")
	ctx, cancel := context.WithTimeout(context.Background(), oneOnOneTimeout)
	defer cancel()

	suffix := randomE2EID(t)
	room1 := "e2e-room-1-" + suffix
	room2 := "e2e-room-2-" + suffix
	room3 := "e2e-room-3-" + suffix
	clients := []struct {
		name          string
		roomIDs       []string
		receivesEvent bool
	}{
		{name: "room-1-only", roomIDs: []string{room1}},
		{name: "rooms-1-and-2", roomIDs: []string{room1, room2}},
		{name: "room-2-only", roomIDs: []string{room2}},
		{name: "room-3-writer", roomIDs: []string{room3}, receivesEvent: true},
		{name: "rooms-2-and-3", roomIDs: []string{room2, room3}, receivesEvent: true},
		{name: "rooms-1-and-3", roomIDs: []string{room1, room3}, receivesEvent: true},
	}

	connections := make([]*websocket.Conn, len(clients))
	for i, client := range clients {
		clientID := client.name + "-" + randomE2EID(t)
		connections[i] = dialE2EClient(t, ctx, endpoint, clientID, client.roomIDs...)
		defer connections[i].Close()
	}

	requestText := "room-3-only-" + randomE2EID(t)
	request := messagev1.SendRequest{
		Type:            messagev1.SendFrameType,
		Version:         messagev1.Version,
		ClientMessageID: "client-message-" + randomE2EID(t),
		RoomID:          room3,
		Payload:         json.RawMessage(fmt.Sprintf(`{"text":%q}`, requestText)),
	}
	if err := request.Validate(); err != nil {
		t.Fatalf("invalid test request: %v", err)
	}
	if err := connections[3].WriteJSON(request); err != nil {
		t.Fatalf("room 3 writer send message: %v", err)
	}

	ack, writerEvent := readAcceptedAckAndEvent(t, connections[3], request.ClientMessageID)
	assertE2EEventMatches(t, writerEvent, ack, request, requestText)

	for i, client := range clients {
		if i == 3 || !client.receivesEvent {
			continue
		}
		event := readRoomEvent(t, connections[i], request.ClientMessageID)
		assertE2EEventMatches(t, event, ack, request, requestText)
	}
	for i, client := range clients {
		if client.receivesEvent {
			continue
		}
		assertNoWebsocketMessage(t, connections[i], client.name)
	}
}

func TestE2EReconnectReceivesRoomMessage(t *testing.T) {
	endpoint := envOr("WS_E2E_URL", "ws://localhost:80/ws")
	ctx, cancel := context.WithTimeout(context.Background(), oneOnOneTimeout)
	defer cancel()

	roomID := "e2e-reconnect-" + randomE2EID(t)
	clientIDs := make([]string, 4)
	connections := make([]*websocket.Conn, len(clientIDs))
	for i := range clientIDs {
		clientIDs[i] = fmt.Sprintf("reconnect-client-%d-%s", i+1, randomE2EID(t))
		connections[i] = dialE2EClient(t, ctx, endpoint, clientIDs[i], roomID)
		defer connections[i].Close()
	}

	closeE2EClient(t, connections[0])
	connections[0] = dialE2EClient(t, ctx, endpoint, clientIDs[0], roomID)
	defer connections[0].Close()

	text := "after-reconnect-" + randomE2EID(t)
	request := messagev1.SendRequest{
		Type:            messagev1.SendFrameType,
		Version:         messagev1.Version,
		ClientMessageID: "client-message-" + randomE2EID(t),
		RoomID:          roomID,
		Payload:         json.RawMessage(fmt.Sprintf(`{"text":%q}`, text)),
	}
	if err := request.Validate(); err != nil {
		t.Fatalf("invalid test request: %v", err)
	}
	if err := connections[1].WriteJSON(request); err != nil {
		t.Fatalf("client 2 send message: %v", err)
	}

	ack, senderEvent := readAcceptedAckAndEvent(t, connections[1], request.ClientMessageID)
	for i := range connections {
		event := senderEvent
		if i != 1 {
			event = readRoomEvent(t, connections[i], request.ClientMessageID)
		}
		assertE2EEventMatches(t, event, ack, request, text)
	}
}

func assertE2EEventMatches(t *testing.T, event *messagev1.Event, ack *messagev1.AcceptedAck, request messagev1.SendRequest, text string) {
	t.Helper()
	if ack.ClientMessageID != request.ClientMessageID || ack.RoomID != request.RoomID {
		t.Fatalf("acknowledgement = %+v; want client message %q in room %q", ack, request.ClientMessageID, request.RoomID)
	}
	if event.ClientMessageID != request.ClientMessageID || event.RoomID != request.RoomID {
		t.Fatalf("delivered event = %+v; want client message %q in room %q", event, request.ClientMessageID, request.RoomID)
	}
	if event.MessageID != ack.MessageID || event.RoomSeq != ack.RoomSeq {
		t.Fatalf("delivered event ID/sequence = %q/%d; want acknowledgement %q/%d", event.MessageID, event.RoomSeq, ack.MessageID, ack.RoomSeq)
	}
	var gotPayload struct {
		Text string `json:"text"`
	}
	if err := json.Unmarshal(event.Payload, &gotPayload); err != nil {
		t.Fatalf("decode delivered payload %s: %v", event.Payload, err)
	}
	if gotPayload.Text != text {
		t.Fatalf("delivered payload text = %q, want %q", gotPayload.Text, text)
	}
}

func dialE2EClient(t *testing.T, ctx context.Context, endpoint, clientID string, roomIDs ...string) *websocket.Conn {
	t.Helper()

	url, err := websocketURL(endpoint, clientID, roomIDs)
	if err != nil {
		t.Fatalf("build WebSocket URL: %v", err)
	}
	conn, response, err := websocket.DefaultDialer.DialContext(ctx, url, nil)
	if err != nil {
		if response != nil && response.Body != nil {
			_ = response.Body.Close()
		}
		t.Fatalf("connect client %q to %s: %v", clientID, url, err)
	}
	if err := conn.SetReadDeadline(time.Now().Add(oneOnOneTimeout)); err != nil {
		conn.Close()
		t.Fatalf("set read deadline: %v", err)
	}
	return conn
}

func assertNoWebsocketMessage(t *testing.T, conn *websocket.Conn, clientName string) {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond)); err != nil {
		t.Fatalf("set no-message deadline for %s: %v", clientName, err)
	}
	_, data, err := conn.ReadMessage()
	if err == nil {
		t.Fatalf("client %s received an unexpected WebSocket frame: %s", clientName, data)
	}
	if timeout, ok := err.(net.Error); !ok || !timeout.Timeout() {
		t.Fatalf("read from non-subscribed client %s: %v", clientName, err)
	}
}

func closeE2EClient(t *testing.T, conn *websocket.Conn) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	if err := conn.WriteControl(
		websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseNormalClosure, "test disconnect"),
		deadline,
	); err != nil {
		t.Fatalf("send client close frame: %v", err)
	}
	if err := conn.SetReadDeadline(deadline); err != nil {
		t.Fatalf("set close-handshake deadline: %v", err)
	}
	_, _, err := conn.ReadMessage()
	if !websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseNoStatusReceived) {
		t.Fatalf("wait for server close frame: %v", err)
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("close client connection: %v", err)
	}
}

func readAcceptedAck(t *testing.T, conn *websocket.Conn, clientMessageID string) *messagev1.AcceptedAck {
	t.Helper()

	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("read accepted acknowledgement: %v", err)
		}
		frame, err := decodeServerFrame(data)
		if err != nil {
			t.Fatalf("decode server frame %s: %v", data, err)
		}
		switch frame.kind {
		case messagev1.AcceptedFrameType:
			if frame.accepted.ClientMessageID != clientMessageID {
				continue
			}
			return frame.accepted
		case messagev1.RejectedFrameType:
			t.Fatalf("message %q rejected with %q", clientMessageID, frame.rejected.ErrorCode)
		}
	}
}

func readAcceptedAckAndEvent(t *testing.T, conn *websocket.Conn, clientMessageID string) (*messagev1.AcceptedAck, *messagev1.Event) {
	t.Helper()
	var ack *messagev1.AcceptedAck
	var event *messagev1.Event
	for ack == nil || event == nil {
		_, data, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("read sender acknowledgement and event: %v", err)
		}
		frame, err := decodeServerFrame(data)
		if err != nil {
			t.Fatalf("decode server frame %s: %v", data, err)
		}
		switch frame.kind {
		case messagev1.AcceptedFrameType:
			if frame.accepted.ClientMessageID == clientMessageID {
				ack = frame.accepted
			}
		case messagev1.RejectedFrameType:
			t.Fatalf("message %q rejected with %q", clientMessageID, frame.rejected.ErrorCode)
		case "event":
			if frame.event.ClientMessageID == clientMessageID {
				event = frame.event
			}
		}
	}
	return ack, event
}

func readRoomEvent(t *testing.T, conn *websocket.Conn, clientMessageID string) *messagev1.Event {
	t.Helper()

	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("read delivered room event: %v", err)
		}
		frame, err := decodeServerFrame(data)
		if err != nil {
			t.Fatalf("decode server frame %s: %v", data, err)
		}
		if frame.kind == "event" && frame.event.ClientMessageID == clientMessageID {
			return frame.event
		}
	}
}

func randomE2EID(t *testing.T) string {
	t.Helper()

	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		t.Fatalf("generate unique test ID: %v", err)
	}
	return hex.EncodeToString(id[:])
}
