module github.com/AGranosik/chat/services/wsclient

go 1.26.3

require (
	github.com/AGranosik/chat/contracts/messages v0.0.0
	github.com/gorilla/websocket v1.5.3
)

replace github.com/AGranosik/chat/contracts/messages => ../../contracts/messages
