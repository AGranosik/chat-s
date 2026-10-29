package transport

import (
	"context"
	"messages/app/transport/ws"
	"net/http"
	"time"
)

func CreateHttpTransport(port string, hub *ws.Ws, readiness func(context.Context) error) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", hub.ServeWS)
	mux.HandleFunc("/health/live", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/health/ready", func(w http.ResponseWriter, r *http.Request) {
		if readiness == nil {
			http.Error(w, "readiness check is unavailable", http.StatusServiceUnavailable)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), time.Second)
		defer cancel()
		if err := readiness(ctx); err != nil {
			http.Error(w, "message database is not ready", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	server := &http.Server{
		Addr:              port,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	return server
}
