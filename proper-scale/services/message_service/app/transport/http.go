package transport

import (
	"log"
	"messages/app/transport"
	"net/http"
)

func CreateHttpTransport(port string, hub *transport.Ws) {
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", hub.ServeWS)

	server := &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	log.Printf("listening", "addr", server.Addr)
	log.Fatal(server.ListenAndServe())
}
