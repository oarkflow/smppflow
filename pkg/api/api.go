package api

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/oarkflow/smppflow/pkg/alerts"
	"github.com/oarkflow/smppflow/pkg/contacts"
	"github.com/oarkflow/smppflow/pkg/manager"
	"github.com/oarkflow/smppflow/pkg/message"
	"github.com/oarkflow/smppflow/pkg/optout"
	"github.com/oarkflow/smppflow/pkg/ratelimit"
	"github.com/oarkflow/smppflow/pkg/reports"
	"github.com/oarkflow/smppflow/pkg/service"
)

type Server struct {
	Service  *service.Service
	Store    message.Store
	Ops      *manager.Operations
	OptOut   *optout.Store
	Contacts *contacts.Store
	Limiter  *ratelimit.Limiter
	Alerts   *alerts.Engine
	mux      *http.ServeMux
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
	a.mux.HandleFunc("/v1/reports/delivery", a.deliveryReport)
	a.mux.HandleFunc("/v1/reports/providers", a.providerReport)
	a.mux.HandleFunc("/v1/reports/tenants", a.tenantReport)
	a.mux.HandleFunc("/v1/reports/errors", a.errorReport)
	a.mux.HandleFunc("/v1/reports/export.ndjson", a.exportNDJSON)
	a.mux.HandleFunc("/v1/manager/queue", a.queueSnapshot)
	a.mux.HandleFunc("/v1/manager/dlq", a.dlqSnapshot)
	a.mux.HandleFunc("/v1/manager/route/simulate", a.routeSimulate)
	a.mux.HandleFunc("/v1/manager/messages/trace", a.traceMessage)
	a.mux.HandleFunc("/v1/manager/tenants", a.tenants)
	a.mux.HandleFunc("/v1/manager/providers", a.providers)
	a.mux.HandleFunc("/v1/manager/binds", a.binds)
	a.mux.HandleFunc("/v1/manager/senders", a.senderApprovals)
	a.mux.HandleFunc("/v1/manager/sessions", a.sessions)
	a.mux.HandleFunc("/v1/manager/audit", a.audit)
	a.mux.HandleFunc("/v1/manager/queue/replay-dlq", a.replayDLQ)
	a.mux.HandleFunc("/v1/manager/queue/cancel", a.cancelQueued)
	a.mux.HandleFunc("/v1/optouts", a.optouts)
	a.mux.HandleFunc("/v1/contacts", a.contacts)
	a.mux.HandleFunc("/v1/ratelimits", a.rateLimits)
	a.mux.HandleFunc("/v1/alerts/open", a.openAlerts)
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

func (a *Server) filter(r *http.Request) reports.Filter {
	q := r.URL.Query()
	f := reports.Filter{TenantID: q.Get("tenant_id"), ProviderID: q.Get("provider_id"), Sender: q.Get("sender"), CampaignID: q.Get("campaign_id"), GroupBy: q.Get("group_by")}
	if v := q.Get("from"); v != "" {
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			f.From = t
		}
	}
	if v := q.Get("to"); v != "" {
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			f.To = t
		}
	}
	return f
}
func (a *Server) deliveryReport(w http.ResponseWriter, r *http.Request) {
	rep, err := reports.GenerateDelivery(r.Context(), a.Store, a.filter(r))
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	writeJSON(w, rep)
}
func (a *Server) providerReport(w http.ResponseWriter, r *http.Request) {
	rep, err := reports.GenerateProvider(r.Context(), a.Store, a.filter(r))
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	writeJSON(w, rep)
}
func (a *Server) tenantReport(w http.ResponseWriter, r *http.Request) {
	rep, err := reports.GenerateTenantUsage(r.Context(), a.Store, a.filter(r))
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	writeJSON(w, rep)
}
func (a *Server) errorReport(w http.ResponseWriter, r *http.Request) {
	rep, err := reports.GenerateErrors(r.Context(), a.Store, a.filter(r))
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	writeJSON(w, rep)
}
func (a *Server) exportNDJSON(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("content-type", "application/x-ndjson")
	_ = reports.WriteNDJSON(r.Context(), w, a.Store, a.filter(r))
}
func (a *Server) queueSnapshot(w http.ResponseWriter, r *http.Request) {
	if a.Ops == nil {
		http.Error(w, "manager ops not configured", 500)
		return
	}
	writeJSON(w, a.Ops.QueueSnapshot())
}
func (a *Server) dlqSnapshot(w http.ResponseWriter, r *http.Request) {
	if a.Ops == nil {
		http.Error(w, "manager ops not configured", 500)
		return
	}
	writeJSON(w, a.Ops.DLQ())
}
func (a *Server) routeSimulate(w http.ResponseWriter, r *http.Request) {
	if a.Ops == nil {
		http.Error(w, "manager ops not configured", 500)
		return
	}
	var req message.SubmitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	sim, err := a.Ops.SimulateRoute(r.Context(), req)
	if err != nil {
		w.WriteHeader(422)
	}
	writeJSON(w, sim)
}
func (a *Server) traceMessage(w http.ResponseWriter, r *http.Request) {
	if a.Ops == nil {
		http.Error(w, "manager ops not configured", 500)
		return
	}
	id := message.ID(r.URL.Query().Get("id"))
	tr, err := a.Ops.TraceMessage(r.Context(), id)
	if err != nil {
		http.Error(w, err.Error(), 404)
		return
	}
	writeJSON(w, tr)
}

func (a *Server) requireOps(w http.ResponseWriter) bool {
	if a.Ops == nil || a.Ops.Store == nil {
		http.Error(w, "manager ops not configured", 500)
		return false
	}
	return true
}
func (a *Server) tenants(w http.ResponseWriter, r *http.Request) {
	if !a.requireOps(w) {
		return
	}
	switch r.Method {
	case http.MethodGet:
		xs, err := a.Ops.Store.ListTenants(r.Context())
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		writeJSON(w, xs)
	case http.MethodPost, http.MethodPut:
		var t manager.Tenant
		if err := json.NewDecoder(r.Body).Decode(&t); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		if err := a.Ops.UpsertTenant(r.Context(), r.Header.Get("X-Actor-ID"), t); err != nil {
			http.Error(w, err.Error(), 422)
			return
		}
		writeJSON(w, map[string]any{"saved": true})
	default:
		http.Error(w, "method not allowed", 405)
	}
}
func (a *Server) providers(w http.ResponseWriter, r *http.Request) {
	if !a.requireOps(w) {
		return
	}
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", 405)
		return
	}
	xs, err := a.Ops.Store.ListProviders(r.Context())
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	writeJSON(w, xs)
}
func (a *Server) binds(w http.ResponseWriter, r *http.Request) {
	if !a.requireOps(w) {
		return
	}
	switch r.Method {
	case http.MethodGet:
		xs, err := a.Ops.Store.ListBinds(r.Context(), r.URL.Query().Get("tenant_id"))
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		writeJSON(w, xs)
	case http.MethodPost, http.MethodPut:
		var b manager.BindAccount
		if err := json.NewDecoder(r.Body).Decode(&b); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		if err := a.Ops.SaveBind(r.Context(), r.Header.Get("X-Actor-ID"), b); err != nil {
			http.Error(w, err.Error(), 422)
			return
		}
		writeJSON(w, map[string]any{"saved": true})
	default:
		http.Error(w, "method not allowed", 405)
	}
}
func (a *Server) senderApprovals(w http.ResponseWriter, r *http.Request) {
	if !a.requireOps(w) {
		return
	}
	switch r.Method {
	case http.MethodGet:
		xs, err := a.Ops.Store.ListSenderApprovals(r.Context(), r.URL.Query().Get("tenant_id"))
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		writeJSON(w, xs)
	case http.MethodPost, http.MethodPut:
		var ap manager.SenderApproval
		if err := json.NewDecoder(r.Body).Decode(&ap); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		if err := a.Ops.Store.SaveSenderApproval(r.Context(), ap); err != nil {
			http.Error(w, err.Error(), 422)
			return
		}
		_ = a.Ops.Store.Audit(r.Context(), manager.AuditLog{TenantID: ap.TenantID, ActorID: r.Header.Get("X-Actor-ID"), Action: "sender_approval.upsert", Resource: "sender_approval", ResourceID: ap.ID})
		writeJSON(w, map[string]any{"saved": true})
	default:
		http.Error(w, "method not allowed", 405)
	}
}
func (a *Server) sessions(w http.ResponseWriter, r *http.Request) {
	if !a.requireOps(w) {
		return
	}
	writeJSON(w, a.Ops.SessionsSnapshot())
}
func (a *Server) audit(w http.ResponseWriter, r *http.Request) {
	if !a.requireOps(w) {
		return
	}
	xs, err := a.Ops.Store.ListAudit(r.Context(), r.URL.Query().Get("tenant_id"), 200)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	writeJSON(w, xs)
}
func (a *Server) replayDLQ(w http.ResponseWriter, r *http.Request) {
	if a.Ops == nil {
		http.Error(w, "manager ops not configured", 500)
		return
	}
	var in struct {
		IDs []string `json:"ids"`
	}
	_ = json.NewDecoder(r.Body).Decode(&in)
	if err := a.Ops.ReplayDLQ(r.Context(), in.IDs...); err != nil {
		http.Error(w, err.Error(), 422)
		return
	}
	writeJSON(w, map[string]any{"replayed": true})
}
func (a *Server) cancelQueued(w http.ResponseWriter, r *http.Request) {
	if a.Ops == nil {
		http.Error(w, "manager ops not configured", 500)
		return
	}
	id := r.URL.Query().Get("id")
	if id == "" {
		var in struct {
			ID string `json:"id"`
		}
		_ = json.NewDecoder(r.Body).Decode(&in)
		id = in.ID
	}
	if err := a.Ops.CancelQueued(r.Context(), id); err != nil {
		http.Error(w, err.Error(), 422)
		return
	}
	writeJSON(w, map[string]any{"cancelled": true})
}
func (a *Server) optouts(w http.ResponseWriter, r *http.Request) {
	if a.OptOut == nil {
		http.Error(w, "optout store not configured", 500)
		return
	}
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, a.OptOut.List(r.Context(), r.URL.Query().Get("tenant_id")))
	case http.MethodPost:
		var rec optout.Record
		if err := json.NewDecoder(r.Body).Decode(&rec); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		if err := a.OptOut.Add(r.Context(), rec); err != nil {
			http.Error(w, err.Error(), 422)
			return
		}
		writeJSON(w, map[string]any{"saved": true})
	case http.MethodDelete:
		if err := a.OptOut.Remove(r.Context(), r.URL.Query().Get("tenant_id"), r.URL.Query().Get("msisdn")); err != nil {
			http.Error(w, err.Error(), 422)
			return
		}
		writeJSON(w, map[string]any{"deleted": true})
	default:
		http.Error(w, "method not allowed", 405)
	}
}
func (a *Server) contacts(w http.ResponseWriter, r *http.Request) {
	if a.Contacts == nil {
		http.Error(w, "contact store not configured", 500)
		return
	}
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, a.Contacts.List(r.Context(), r.URL.Query().Get("tenant_id")))
	case http.MethodPost, http.MethodPut:
		var c contacts.Contact
		if err := json.NewDecoder(r.Body).Decode(&c); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		if err := a.Contacts.Upsert(r.Context(), c); err != nil {
			http.Error(w, err.Error(), 422)
			return
		}
		writeJSON(w, map[string]any{"saved": true})
	default:
		http.Error(w, "method not allowed", 405)
	}
}
func (a *Server) rateLimits(w http.ResponseWriter, r *http.Request) {
	if a.Limiter == nil {
		http.Error(w, "rate limiter not configured", 500)
		return
	}
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, a.Limiter.Snapshot())
	case http.MethodPost, http.MethodPut:
		var p ratelimit.Policy
		if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		if err := a.Limiter.SetPolicy(p); err != nil {
			http.Error(w, err.Error(), 422)
			return
		}
		writeJSON(w, map[string]any{"saved": true})
	default:
		http.Error(w, "method not allowed", 405)
	}
}
func (a *Server) openAlerts(w http.ResponseWriter, r *http.Request) {
	if a.Alerts == nil {
		http.Error(w, "alerts not configured", 500)
		return
	}
	writeJSON(w, a.Alerts.Open())
}
