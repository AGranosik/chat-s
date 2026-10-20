package transport

import (
	"messages/app/transport/ws"
	"net/http"
)

func CreateHttpTransport(port string, hub *ws.Ws) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", hub.ServeWS)

	server := &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	return server
}
