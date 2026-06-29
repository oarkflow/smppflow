package api

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/oarkflow/smppflow/message"
	"github.com/oarkflow/smppflow/reports"
	"github.com/oarkflow/smppflow/service"
)

type Server struct {
	Service *service.Service
	Store   message.Store
	mux     *http.ServeMux
}

func New(svc *service.Service, st message.Store) *Server {
	a := &Server{Service: svc, Store: st, mux: http.NewServeMux()}
	a.routes()
	return a
}
func (a *Server) Handler() http.Handler { return a.mux }
func (a *Server) routes() {
	a.mux.HandleFunc("/v1/messages/send", a.send)
	a.mux.HandleFunc("/v1/messages/enqueue", a.enqueue)
	a.mux.HandleFunc("/v1/messages", a.messages)
	a.mux.HandleFunc("/v1/dlrs", a.dlrs)
	a.mux.HandleFunc("/v1/reports/summary", a.report)
	a.mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200); _, _ = w.Write([]byte("ok")) })
}
func (a *Server) send(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", 405)
		return
	}
	var req message.SubmitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	res, err := a.Service.Send(r.Context(), req)
	if err != nil {
		http.Error(w, err.Error(), 502)
		return
	}
	writeJSON(w, res)
}
func (a *Server) enqueue(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", 405)
		return
	}
	var in struct {
		message.SubmitRequest
		Priority int
		RunAt    time.Time
	}
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	if err := a.Service.Enqueue(r.Context(), in.SubmitRequest, in.Priority, in.RunAt); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	writeJSON(w, map[string]any{"queued": true})
}
func (a *Server) messages(w http.ResponseWriter, r *http.Request) {
	xs, err := a.Store.ListMessages(r.Context())
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	writeJSON(w, xs)
}
func (a *Server) dlrs(w http.ResponseWriter, r *http.Request) {
	xs, err := a.Store.ListDLR(r.Context())
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	writeJSON(w, xs)
}
func (a *Server) report(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("content-type", "application/json")
	_ = reports.WriteJSON(context.Background(), w, a.Store)
}
func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("content-type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
