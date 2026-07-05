package routing

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/oarkflow/smppflow/pkg/compliance"
	"github.com/oarkflow/smppflow/pkg/message"
	"github.com/oarkflow/smppflow/pkg/provider"
)

const (
	RejectCompliance        RejectionReason = "compliance_rejected"
	RejectRating            RejectionReason = "rating_failed"
	RejectBalance           RejectionReason = "insufficient_balance"
	RejectCircuitOpen       RejectionReason = "circuit_open"
	RejectCoverage          RejectionReason = "provider_coverage_unsupported"
	RejectGlobalRouteDenied RejectionReason = "global_route_denied"
	RejectCapacityReserved  RejectionReason = "capacity_reservation_failed"
)

type RouteMode string

const (
	RouteInitial   RouteMode = "initial"
	RouteRetry     RouteMode = "retry"
	RouteFailover  RouteMode = "failover"
	RouteReconcile RouteMode = "reconcile"
)

type PlanOptions struct {
	Mode               RouteMode
	DryRun             bool
	MessageID          message.ID
	Now                time.Time
	ExcludeProviderID  string
	OriginalProviderID string
	AcceptedByProvider bool
}

type RoutePlan struct {
	Request     message.SubmitRequest  `json:"request"`
	Mode        RouteMode              `json:"mode"`
	Candidates  []CandidateExplanation `json:"candidates"`
	Rejected    []CandidateExplanation `json:"rejected"`
	Selected    *CandidateExplanation  `json:"selected,omitempty"`
	Decision    *Decision              `json:"-"`
	GeneratedAt time.Time              `json:"generated_at"`
}

type RouteRepository interface {
	SaveRoutes(ctx context.Context, cfg RouterConfig) error
	LoadRoutes(ctx context.Context) (RouterConfig, error)
}

type SnapshotStore interface {
	SaveSnapshot(ctx context.Context, snapshot RouteDecisionSnapshot) error
	ListSnapshots(ctx context.Context, tenantID string, limit int) ([]RouteDecisionSnapshot, error)
}

type RouterConfig struct {
	Providers []provider.Profile `json:"providers"`
	Rules     []Rule             `json:"rules"`
	Version   int64              `json:"version"`
}

type ValidationSeverity string

const (
	ValidationError   ValidationSeverity = "error"
	ValidationWarning ValidationSeverity = "warning"
)

type RouteValidationIssue struct {
	Severity ValidationSeverity `json:"severity"`
	RuleID   string             `json:"rule_id,omitempty"`
	Code     string             `json:"code"`
	Message  string             `json:"message"`
}

type FileRouteRepository struct{ Path string }

func NewFileRouteRepository(path string) *FileRouteRepository {
	return &FileRouteRepository{Path: path}
}
func (f *FileRouteRepository) SaveRoutes(ctx context.Context, cfg RouterConfig) error {
	_ = ctx
	if f == nil || f.Path == "" {
		return errors.New("route repository path required")
	}
	b, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(f.Path, b, 0600)
}
func (f *FileRouteRepository) LoadRoutes(ctx context.Context) (RouterConfig, error) {
	_ = ctx
	if f == nil || f.Path == "" {
		return RouterConfig{}, errors.New("route repository path required")
	}
	b, err := os.ReadFile(f.Path)
	if err != nil {
		return RouterConfig{}, err
	}
	var cfg RouterConfig
	if err := json.Unmarshal(b, &cfg); err != nil {
		return RouterConfig{}, err
	}
	return cfg, nil
}

// SQLRouteRepository stores versioned route configuration in database/sql.
// It is intentionally driver neutral and uses a compact JSON payload so that
// PostgreSQL, MySQL and SQLite deployments can share the same repository.
type SQLRouteRepository struct{ DB *sql.DB }

func NewSQLRouteRepository(db *sql.DB) *SQLRouteRepository { return &SQLRouteRepository{DB: db} }

func (s *SQLRouteRepository) Migrate(ctx context.Context) error {
	if s == nil || s.DB == nil {
		return errors.New("nil route repository db")
	}
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS smpp_route_configs (id VARCHAR(64) PRIMARY KEY, version BIGINT, payload TEXT NOT NULL, created_at TIMESTAMP)`,
		`CREATE TABLE IF NOT EXISTS smpp_route_audit_logs (id VARCHAR(128) PRIMARY KEY, version BIGINT, action VARCHAR(64), actor_id VARCHAR(128), payload TEXT, created_at TIMESTAMP)`,
	}
	for _, st := range stmts {
		if _, err := s.DB.ExecContext(ctx, st); err != nil {
			return err
		}
	}
	return nil
}

func (s *SQLRouteRepository) SaveRoutes(ctx context.Context, cfg RouterConfig) error {
	if s == nil || s.DB == nil {
		return errors.New("nil route repository db")
	}
	if cfg.Version == 0 {
		cfg.Version = time.Now().UnixNano()
	}
	b, err := json.Marshal(cfg)
	if err != nil {
		return err
	}
	if err := s.Migrate(ctx); err != nil {
		return err
	}
	_, _ = s.DB.ExecContext(ctx, `DELETE FROM smpp_route_configs WHERE id=?`, "active")
	_, err = s.DB.ExecContext(ctx, `INSERT INTO smpp_route_configs(id,version,payload,created_at) VALUES(?,?,?,?)`, "active", cfg.Version, string(b), time.Now())
	if err != nil {
		return err
	}
	_, _ = s.DB.ExecContext(ctx, `INSERT INTO smpp_route_audit_logs(id,version,action,payload,created_at) VALUES(?,?,?,?,?)`, fmt.Sprintf("route-audit-%d", time.Now().UnixNano()), cfg.Version, "save", string(b), time.Now())
	return nil
}

func (s *SQLRouteRepository) LoadRoutes(ctx context.Context) (RouterConfig, error) {
	if s == nil || s.DB == nil {
		return RouterConfig{}, errors.New("nil route repository db")
	}
	if err := s.Migrate(ctx); err != nil {
		return RouterConfig{}, err
	}
	row := s.DB.QueryRowContext(ctx, `SELECT payload FROM smpp_route_configs WHERE id=?`, "active")
	var payload string
	if err := row.Scan(&payload); err != nil {
		return RouterConfig{}, err
	}
	var cfg RouterConfig
	if err := json.Unmarshal([]byte(payload), &cfg); err != nil {
		return RouterConfig{}, err
	}
	return cfg, nil
}

type MemorySnapshotStore struct {
	mu   sync.RWMutex
	rows []RouteDecisionSnapshot
}

func NewMemorySnapshotStore() *MemorySnapshotStore { return &MemorySnapshotStore{} }
func (s *MemorySnapshotStore) SaveSnapshot(ctx context.Context, snap RouteDecisionSnapshot) error {
	_ = ctx
	s.mu.Lock()
	s.rows = append(s.rows, snap)
	s.mu.Unlock()
	return nil
}
func (s *MemorySnapshotStore) ListSnapshots(ctx context.Context, tenantID string, limit int) ([]RouteDecisionSnapshot, error) {
	_ = ctx
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]RouteDecisionSnapshot, 0, len(s.rows))
	for i := len(s.rows) - 1; i >= 0; i-- {
		if tenantID == "" || s.rows[i].TenantID == tenantID {
			out = append(out, s.rows[i])
			if limit > 0 && len(out) >= limit {
				break
			}
		}
	}
	return out, nil
}

// SQLSnapshotStore persists every route decision for later support, billing,
// reconciliation and dispute/debug analysis.
type SQLSnapshotStore struct{ DB *sql.DB }

func NewSQLSnapshotStore(db *sql.DB) *SQLSnapshotStore { return &SQLSnapshotStore{DB: db} }

func (s *SQLSnapshotStore) Migrate(ctx context.Context) error {
	if s == nil || s.DB == nil {
		return errors.New("nil snapshot db")
	}
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS smpp_route_snapshots (id VARCHAR(128) PRIMARY KEY, message_id VARCHAR(128), tenant_id VARCHAR(128), provider_id VARCHAR(128), rule_id VARCHAR(128), route_version BIGINT, objective VARCHAR(64), strategy VARCHAR(64), score DOUBLE PRECISION, payload TEXT NOT NULL, created_at TIMESTAMP)`,
		`CREATE INDEX IF NOT EXISTS idx_smpp_route_snapshots_tenant ON smpp_route_snapshots(tenant_id, created_at)`,
		`CREATE INDEX IF NOT EXISTS idx_smpp_route_snapshots_msg ON smpp_route_snapshots(message_id)`,
	}
	for _, st := range stmts {
		if _, err := s.DB.ExecContext(ctx, st); err != nil {
			return err
		}
	}
	return nil
}

func (s *SQLSnapshotStore) SaveSnapshot(ctx context.Context, snap RouteDecisionSnapshot) error {
	if s == nil || s.DB == nil {
		return errors.New("nil snapshot db")
	}
	if snap.ID == "" {
		snap.ID = fmt.Sprintf("route-%d", time.Now().UnixNano())
	}
	if snap.CreatedAt.IsZero() {
		snap.CreatedAt = time.Now()
	}
	b, err := json.Marshal(snap)
	if err != nil {
		return err
	}
	if err := s.Migrate(ctx); err != nil {
		return err
	}
	_, _ = s.DB.ExecContext(ctx, `DELETE FROM smpp_route_snapshots WHERE id=?`, snap.ID)
	_, err = s.DB.ExecContext(ctx, `INSERT INTO smpp_route_snapshots(id,message_id,tenant_id,provider_id,rule_id,route_version,objective,strategy,score,payload,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)`, snap.ID, snap.MessageID, snap.TenantID, snap.ProviderID, snap.RuleID, snap.RouteVersion, string(snap.Objective), string(snap.Strategy), snap.Score, string(b), snap.CreatedAt)
	return err
}

func (s *SQLSnapshotStore) ListSnapshots(ctx context.Context, tenantID string, limit int) ([]RouteDecisionSnapshot, error) {
	if s == nil || s.DB == nil {
		return nil, errors.New("nil snapshot db")
	}
	if err := s.Migrate(ctx); err != nil {
		return nil, err
	}
	q := `SELECT payload FROM smpp_route_snapshots`
	args := []any{}
	if tenantID != "" {
		q += ` WHERE tenant_id=?`
		args = append(args, tenantID)
	}
	q += ` ORDER BY created_at DESC`
	if limit > 0 {
		q += fmt.Sprintf(` LIMIT %d`, limit)
	}
	rows, err := s.DB.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []RouteDecisionSnapshot{}
	for rows.Next() {
		var payload string
		if err := rows.Scan(&payload); err != nil {
			return nil, err
		}
		var snap RouteDecisionSnapshot
		if err := json.Unmarshal([]byte(payload), &snap); err != nil {
			return nil, err
		}
		out = append(out, snap)
	}
	return out, rows.Err()
}

type ComplianceFilter interface {
	Evaluate(ctx context.Context, req message.SubmitRequest, country string) compliance.RuleDecision
}

type RouteRate struct {
	ProviderID   string `json:"provider_id"`
	Currency     string `json:"currency"`
	CostMicros   int64  `json:"cost_micros"`
	SellMicros   int64  `json:"sell_micros"`
	MarginMicros int64  `json:"margin_micros"`
	Billable     bool   `json:"billable"`
}

type RatingFilter interface {
	RateRoute(ctx context.Context, req message.SubmitRequest, providerID string, segments int) (RouteRate, error)
}

type BalanceFilter interface {
	CanReserve(ctx context.Context, tenantID string, rate RouteRate) error
}

type Coverage struct {
	ProviderID            string `json:"provider_id"`
	Country               string `json:"country"`
	Operator              string `json:"operator"`
	MCC                   string `json:"mcc"`
	MNC                   string `json:"mnc"`
	MessageType           string `json:"message_type"`
	SupportsAlphaSender   bool   `json:"supports_alpha_sender"`
	SupportsNumericSender bool   `json:"supports_numeric_sender"`
	SupportsDLR           bool   `json:"supports_dlr"`
	SupportsUnicode       bool   `json:"supports_unicode"`
	SupportsFlash         bool   `json:"supports_flash"`
	SupportsLongSMS       bool   `json:"supports_long_sms"`
	SupportsTemplateID    bool   `json:"supports_template_id"`
	Enabled               bool   `json:"enabled"`
}

type CoverageMatrix struct {
	mu   sync.RWMutex
	rows []Coverage
}

func NewCoverageMatrix() *CoverageMatrix { return &CoverageMatrix{} }
func (m *CoverageMatrix) Upsert(c Coverage) {
	if c.Enabled == false { /* explicit false allowed */
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := range m.rows {
		if coverageKey(m.rows[i]) == coverageKey(c) {
			m.rows[i] = c
			return
		}
	}
	m.rows = append(m.rows, c)
}
func (m *CoverageMatrix) Allows(p provider.Profile, req message.SubmitRequest) bool {
	if m == nil {
		return true
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if len(m.rows) == 0 {
		return true
	}
	bestScore := -1
	var best Coverage
	for _, c := range m.rows {
		if c.ProviderID != "" && c.ProviderID != p.ID {
			continue
		}
		score := 0
		if c.ProviderID != "" {
			score += 16
		}
		if c.Country != "" {
			if !eqFold(c.Country, req.Meta["country"]) {
				continue
			}
			score += 8
		}
		if c.Operator != "" {
			if !eqFold(c.Operator, req.Meta["operator"]) {
				continue
			}
			score += 4
		}
		if c.MCC != "" {
			if !eqFold(c.MCC, req.Meta["mcc"]) {
				continue
			}
			score += 2
		}
		if c.MNC != "" {
			if !eqFold(c.MNC, req.Meta["mnc"]) {
				continue
			}
			score += 2
		}
		if c.MessageType != "" {
			if !eqFold(c.MessageType, req.Meta["message_type"]) {
				continue
			}
			score += 1
		}
		if score > bestScore {
			bestScore = score
			best = c
		}
	}
	if bestScore < 0 {
		return true
	}
	if !best.Enabled {
		return false
	}
	senderAlpha := hasLetter(req.From)
	if senderAlpha && !best.SupportsAlphaSender {
		return false
	}
	if !senderAlpha && !best.SupportsNumericSender {
		return false
	}
	if req.RegisteredDelivery != 0 && !best.SupportsDLR {
		return false
	}
	if req.TemplateID != "" && !best.SupportsTemplateID {
		return false
	}
	if req.Meta["unicode"] == "true" && !best.SupportsUnicode {
		return false
	}
	if req.Meta["flash"] == "true" && !best.SupportsFlash {
		return false
	}
	if estimateSegments(req) > 1 && !best.SupportsLongSMS {
		return false
	}
	return true
}
func coverageKey(c Coverage) string {
	return c.ProviderID + "\x00" + strings.ToUpper(c.Country) + "\x00" + strings.ToUpper(c.Operator) + "\x00" + c.MCC + "\x00" + c.MNC + "\x00" + strings.ToLower(c.MessageType)
}

type CapacityReservation struct {
	ID          string
	ProviderID  string
	TenantID    string
	RouteID     string
	BindKey     string
	WindowSlots int
	CreatedAt   time.Time
}

type CapacityManager interface {
	Check(ctx context.Context, req message.SubmitRequest, d Decision) []RejectionReason
	Reserve(ctx context.Context, req message.SubmitRequest, d Decision) (CapacityReservation, []RejectionReason)
	Commit(ctx context.Context, reservationID string) error
	Release(ctx context.Context, reservationID string) error
}

type MemoryCapacityManager struct {
	mu           sync.Mutex
	windows      map[string]int
	reservations map[string]CapacityReservation
}

func NewMemoryCapacityManager() *MemoryCapacityManager {
	return &MemoryCapacityManager{windows: map[string]int{}, reservations: map[string]CapacityReservation{}}
}
func (m *MemoryCapacityManager) Check(ctx context.Context, req message.SubmitRequest, d Decision) []RejectionReason {
	_ = ctx
	_ = req
	m.mu.Lock()
	defer m.mu.Unlock()
	key := capacityBindKey(req, d)
	if d.Provider.WindowSize > 0 && m.windows[key] >= d.Provider.WindowSize {
		return []RejectionReason{RejectWindowFull}
	}
	return nil
}
func (m *MemoryCapacityManager) Reserve(ctx context.Context, req message.SubmitRequest, d Decision) (CapacityReservation, []RejectionReason) {
	_ = ctx
	m.mu.Lock()
	defer m.mu.Unlock()
	slots := 1
	if estimateSegments(req) > 1 {
		slots = 1
	} // submit_sm window consumes one PDU at a time; segment dispatch may reserve repeatedly in caller.
	key := capacityBindKey(req, d)
	if d.Provider.WindowSize > 0 && m.windows[key]+slots > d.Provider.WindowSize {
		return CapacityReservation{}, []RejectionReason{RejectWindowFull}
	}
	id := fmt.Sprintf("cap-%d", time.Now().UnixNano())
	res := CapacityReservation{ID: id, ProviderID: d.Provider.ID, TenantID: req.TenantID, RouteID: d.Rule.ID, BindKey: key, WindowSlots: slots, CreatedAt: time.Now()}
	m.windows[key] += slots
	m.reservations[id] = res
	return res, nil
}
func (m *MemoryCapacityManager) Commit(ctx context.Context, id string) error {
	_ = ctx
	return m.Release(ctx, id)
}
func (m *MemoryCapacityManager) Release(ctx context.Context, id string) error {
	_ = ctx
	m.mu.Lock()
	defer m.mu.Unlock()
	res, ok := m.reservations[id]
	if !ok {
		return nil
	}
	if res.WindowSlots > 0 {
		key := res.BindKey
		if key == "" {
			key = res.ProviderID
		}
		m.windows[key] -= res.WindowSlots
		if m.windows[key] < 0 {
			m.windows[key] = 0
		}
	}
	delete(m.reservations, id)
	return nil
}

type CircuitState string

const (
	CircuitClosed   CircuitState = "closed"
	CircuitOpen     CircuitState = "open"
	CircuitHalfOpen CircuitState = "half_open"
)

type CircuitSnapshot struct {
	Key       string       `json:"key"`
	State     CircuitState `json:"state"`
	Failures  int          `json:"failures"`
	Successes int          `json:"successes"`
	OpenUntil time.Time    `json:"open_until"`
}
type CircuitBreaker struct {
	mu               sync.Mutex
	rows             map[string]*CircuitSnapshot
	FailureThreshold int
	SuccessThreshold int
	OpenDuration     time.Duration
	now              func() time.Time
}

func NewCircuitBreaker() *CircuitBreaker {
	return &CircuitBreaker{rows: map[string]*CircuitSnapshot{}, FailureThreshold: 5, SuccessThreshold: 2, OpenDuration: 30 * time.Second, now: time.Now}
}
func (c *CircuitBreaker) Allow(key string) bool {
	if c == nil {
		return true
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	s := c.get(key)
	now := c.now()
	if s.State == CircuitOpen && now.After(s.OpenUntil) {
		s.State = CircuitHalfOpen
		s.Failures = 0
		s.Successes = 0
	}
	return s.State != CircuitOpen
}
func (c *CircuitBreaker) MarkSuccess(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := c.get(key)
	s.Successes++
	if s.State == CircuitHalfOpen && s.Successes >= max(1, c.SuccessThreshold) {
		s.State = CircuitClosed
		s.Failures = 0
		s.Successes = 0
	}
}
func (c *CircuitBreaker) MarkFailure(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := c.get(key)
	s.Failures++
	if s.Failures >= max(1, c.FailureThreshold) {
		s.State = CircuitOpen
		s.OpenUntil = c.now().Add(c.OpenDuration)
	}
}
func (c *CircuitBreaker) Snapshot() []CircuitSnapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]CircuitSnapshot, 0, len(c.rows))
	for _, v := range c.rows {
		out = append(out, *v)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out
}
func (c *CircuitBreaker) get(key string) *CircuitSnapshot {
	if key == "" {
		key = "global"
	}
	s := c.rows[key]
	if s == nil {
		s = &CircuitSnapshot{Key: key, State: CircuitClosed}
		c.rows[key] = s
	}
	return s
}

type SLAKey struct{ ProviderID, Country, Operator, MessageType string }
type SLAStat struct {
	Key          SLAKey        `json:"key"`
	DeliveryRate float64       `json:"delivery_rate"`
	P95Latency   time.Duration `json:"p95_latency"`
	ErrorRate    float64       `json:"error_rate"`
	SampleSize   int64         `json:"sample_size"`
	UpdatedAt    time.Time     `json:"updated_at"`
}
type SLAStats struct {
	mu   sync.RWMutex
	rows map[SLAKey]SLAStat
}

func NewSLAStats() *SLAStats { return &SLAStats{rows: map[SLAKey]SLAStat{}} }
func (s *SLAStats) Upsert(st SLAStat) {
	s.mu.Lock()
	if st.UpdatedAt.IsZero() {
		st.UpdatedAt = time.Now()
	}
	s.rows[st.Key] = st
	s.mu.Unlock()
}
func (s *SLAStats) Lookup(pid string, req message.SubmitRequest) (SLAStat, bool) {
	if s == nil {
		return SLAStat{}, false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	keys := []SLAKey{{pid, req.Meta["country"], req.Meta["operator"], req.Meta["message_type"]}, {pid, req.Meta["country"], "", req.Meta["message_type"]}, {pid, req.Meta["country"], req.Meta["operator"], ""}, {pid, req.Meta["country"], "", ""}, {pid, "", "", ""}}
	for _, k := range keys {
		if v, ok := s.rows[k]; ok {
			return v, true
		}
	}
	return SLAStat{}, false
}
func (s *SLAStats) Record(pid string, req message.SubmitRequest, success bool, latency time.Duration) {
	if s == nil || pid == "" {
		return
	}
	key := SLAKey{ProviderID: pid, Country: strings.ToUpper(req.Meta["country"]), Operator: strings.ToUpper(req.Meta["operator"]), MessageType: strings.ToLower(req.Meta["message_type"])}
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.rows[key]
	st.Key = key
	n := float64(st.SampleSize)
	if st.SampleSize == 0 {
		if success {
			st.DeliveryRate = 1
		} else {
			st.DeliveryRate = 0
		}
		if !success {
			st.ErrorRate = 1
		}
		st.P95Latency = latency
	} else {
		if success {
			st.DeliveryRate = (st.DeliveryRate*n + 1) / (n + 1)
			st.ErrorRate = (st.ErrorRate * n) / (n + 1)
		} else {
			st.DeliveryRate = (st.DeliveryRate * n) / (n + 1)
			st.ErrorRate = (st.ErrorRate*n + 1) / (n + 1)
		}
		if latency > st.P95Latency {
			st.P95Latency = latency
		} else if latency > 0 {
			st.P95Latency = time.Duration((int64(st.P95Latency)*9 + int64(latency)) / 10)
		}
	}
	st.SampleSize++
	st.UpdatedAt = time.Now()
	s.rows[key] = st
}

type routerExtra struct {
	repo                       RouteRepository
	snapStore                  SnapshotStore
	capacity                   CapacityManager
	compliance                 ComplianceFilter
	rating                     RatingFilter
	balance                    BalanceFilter
	coverage                   *CoverageMatrix
	circuit                    *CircuitBreaker
	sla                        *SLAStats
	requireActiveSession       bool
	allowGlobalRulesForTenants bool
	trie                       *PrefixTrie
}

var extrasMu sync.Mutex
var extras = map[*Router]*routerExtra{}

func extra(r *Router) *routerExtra {
	extrasMu.Lock()
	defer extrasMu.Unlock()
	e := extras[r]
	if e == nil {
		e = &routerExtra{capacity: NewMemoryCapacityManager(), snapStore: NewMemorySnapshotStore(), coverage: NewCoverageMatrix(), circuit: NewCircuitBreaker(), sla: NewSLAStats()}
		extras[r] = e
	}
	return e
}

func (r *Router) SetRouteRepository(repo RouteRepository) { extra(r).repo = repo }
func (r *Router) SetSnapshotStore(store SnapshotStore) {
	if store != nil {
		extra(r).snapStore = store
	}
}
func (r *Router) SetCapacityManager(cm CapacityManager) {
	if cm != nil {
		extra(r).capacity = cm
	}
}
func (r *Router) SetComplianceFilter(c ComplianceFilter) { extra(r).compliance = c }
func (r *Router) SetRatingFilter(f RatingFilter)         { extra(r).rating = f }
func (r *Router) SetBalanceFilter(f BalanceFilter)       { extra(r).balance = f }
func (r *Router) SetCoverageMatrix(m *CoverageMatrix) {
	if m != nil {
		extra(r).coverage = m
	}
}
func (r *Router) SetCircuitBreaker(c *CircuitBreaker) {
	if c != nil {
		extra(r).circuit = c
	}
}
func (r *Router) SetSLAStats(s *SLAStats) {
	if s != nil {
		extra(r).sla = s
	}
}
func (r *Router) RequireActiveSession(v bool)       { extra(r).requireActiveSession = v }
func (r *Router) AllowGlobalRulesForTenants(v bool) { extra(r).allowGlobalRulesForTenants = v }

func (r *Router) CommitReservation(ctx context.Context, reservationID string) error {
	if reservationID == "" {
		return nil
	}
	e := extra(r)
	if e.capacity == nil {
		return nil
	}
	return e.capacity.Commit(ctx, reservationID)
}
func (r *Router) ReleaseReservation(ctx context.Context, reservationID string) error {
	if reservationID == "" {
		return nil
	}
	e := extra(r)
	if e.capacity == nil {
		return nil
	}
	return e.capacity.Release(ctx, reservationID)
}
func (r *Router) ReservationID(d Decision) string {
	if d.Provider.Meta == nil {
		return ""
	}
	return d.Provider.Meta["capacity_reservation_id"]
}
func (r *Router) MarkRouteSuccess(req message.SubmitRequest, providerID string, latency time.Duration) {
	if providerID == "" {
		return
	}
	e := extra(r)
	if e.circuit != nil {
		e.circuit.MarkSuccess(circuitKey(providerID, req))
	}
	r.mu.Lock()
	st := r.stats[providerID]
	st.ProviderID = providerID
	st.Successes++
	if latency > 0 {
		st.AvgLatency = latency
	}
	if st.QualityScore == 0 {
		st.QualityScore = 80
	}
	if st.DeliveryRate == 0 {
		st.DeliveryRate = .98
	}
	st.UpdatedAt = time.Now()
	r.stats[providerID] = st
	r.mu.Unlock()
	if e.sla != nil {
		e.sla.Record(providerID, req, true, latency)
	}
}
func (r *Router) MarkRouteFailure(req message.SubmitRequest, providerID string, latency time.Duration) {
	if providerID == "" {
		return
	}
	e := extra(r)
	if e.circuit != nil {
		e.circuit.MarkFailure(circuitKey(providerID, req))
	}
	r.mu.Lock()
	st := r.stats[providerID]
	st.ProviderID = providerID
	st.Failures++
	if latency > 0 {
		st.AvgLatency = latency
	}
	if st.QualityScore > 1 {
		st.QualityScore -= 1
	}
	st.UpdatedAt = time.Now()
	r.stats[providerID] = st
	r.mu.Unlock()
	if e.sla != nil {
		e.sla.Record(providerID, req, false, latency)
	}
}
func (r *Router) AttachSnapshotMessageIDPersistent(ctx context.Context, snapshotID string, msgID message.ID) {
	r.AttachSnapshotMessageID(snapshotID, msgID)
	if snapshotID == "" || msgID == "" {
		return
	}
	for _, snap := range r.Snapshots() {
		if snap.ID == snapshotID {
			snap.MessageID = string(msgID)
			_ = extra(r).snapStore.SaveSnapshot(ctx, snap)
			return
		}
	}
}

func (r *Router) SaveRoutes(ctx context.Context) error {
	e := extra(r)
	if e.repo == nil {
		return errors.New("route repository not configured")
	}
	r.mu.RLock()
	cfg := RouterConfig{Providers: r.Providers(), Rules: r.Rules(), Version: r.routeVersion}
	r.mu.RUnlock()
	return e.repo.SaveRoutes(ctx, cfg)
}
func (r *Router) LoadRoutes(ctx context.Context) error {
	e := extra(r)
	if e.repo == nil {
		return errors.New("route repository not configured")
	}
	cfg, err := e.repo.LoadRoutes(ctx)
	if err != nil {
		return err
	}
	return r.ApplyConfig(cfg)
}
func (r *Router) ApplyConfig(cfg RouterConfig) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.providers = map[string]provider.Profile{}
	r.states = map[string]ProviderState{}
	r.stats = map[string]ProviderStats{}
	r.rules = nil
	for _, p := range cfg.Providers {
		if p.ID == "" {
			return errors.New("provider id required")
		}
		if p.TPS <= 0 {
			p.TPS = 100
		}
		if p.WindowSize <= 0 {
			p.WindowSize = 16
		}
		r.providers[p.ID] = p
		r.states[p.ID] = stateFromProfile(p)
		r.stats[p.ID] = ProviderStats{ProviderID: p.ID, QualityScore: 80, DeliveryRate: .98, MaxTPS: p.TPS, UpdatedAt: time.Now()}
	}
	for _, rule := range cfg.Rules {
		if err := r.validateRuleLocked(rule); err != nil {
			return err
		}
		if rule.Priority == 0 {
			rule.Priority = 1000
		}
		if rule.Weight <= 0 {
			rule.Weight = 1
		}
		rule.Enabled = !rule.Disabled
		r.rules = append(r.rules, rule)
	}
	if cfg.Version > r.routeVersion {
		r.routeVersion = cfg.Version
	} else {
		r.routeVersion++
	}
	r.rebuildLocked()
	return nil
}

func (r *Router) ValidateRule(rule Rule) error {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.validateRuleLocked(rule)
}
func (r *Router) ValidateAll() error {
	issues := r.ValidateAllDetailed()
	for _, issue := range issues {
		if issue.Severity == ValidationError {
			return errors.New(issue.Message)
		}
	}
	return nil
}

func (r *Router) ValidateAllDetailed() []RouteValidationIssue {
	r.mu.RLock()
	defer r.mu.RUnlock()
	issues := []RouteValidationIssue{}
	seen := map[string]Rule{}
	for _, rule := range r.rules {
		if err := r.validateRuleLocked(rule); err != nil {
			issues = append(issues, RouteValidationIssue{Severity: ValidationError, RuleID: rule.ID, Code: "invalid_rule", Message: err.Error()})
			continue
		}
		if rule.TenantID == "" {
			issues = append(issues, RouteValidationIssue{Severity: ValidationWarning, RuleID: rule.ID, Code: "global_route", Message: "global route exists; enable AllowGlobalRulesForTenants only intentionally"})
		}
		if len(rulePrefixes(rule)) == 0 && rule.Country == "" && rule.Operator == "" && rule.Sender == "" && len(rule.MessageTypes) == 0 {
			issues = append(issues, RouteValidationIssue{Severity: ValidationWarning, RuleID: rule.ID, Code: "broad_route", Message: "route has no destination/message/sender scope and may catch more traffic than intended"})
		}
		if rule.MaxCost > 0 && extra(r).rating == nil {
			issues = append(issues, RouteValidationIssue{Severity: ValidationWarning, RuleID: rule.ID, Code: "static_cost", Message: "rule uses MaxCost without rating filter; only static provider cost is available"})
		}
		if st := r.states[rule.ProviderID]; st == ProviderDown || st == ProviderMaintenance || st == ProviderSuspended {
			issues = append(issues, RouteValidationIssue{Severity: ValidationWarning, RuleID: rule.ID, Code: "inactive_provider", Message: "active rule points to non-active provider"})
		}
		sig := routeSignature(rule)
		if prev, ok := seen[sig]; ok {
			issues = append(issues, RouteValidationIssue{Severity: ValidationError, RuleID: rule.ID, Code: "duplicate_scope", Message: fmt.Sprintf("duplicate route scope: %s and %s", prev.ID, rule.ID)})
		}
		seen[sig] = rule
	}
	for i := 0; i < len(r.rules); i++ {
		for j := i + 1; j < len(r.rules); j++ {
			a, b := r.rules[i], r.rules[j]
			if a.Priority != b.Priority || a.TenantID != b.TenantID || !eqFold(a.Country, b.Country) || !eqFold(a.Operator, b.Operator) || a.Sender != b.Sender {
				continue
			}
			if prefixesOverlap(rulePrefixes(a), rulePrefixes(b)) && a.ProviderID != b.ProviderID && a.Objective != ObjectiveBalanced {
				issues = append(issues, RouteValidationIssue{Severity: ValidationWarning, RuleID: a.ID, Code: "overlap", Message: fmt.Sprintf("route prefix overlaps %s at same priority; verify this is intended", b.ID)})
			}
		}
	}
	return issues
}
func (r *Router) validateRuleLocked(rule Rule) error {
	if rule.ID == "" {
		return errors.New("rule id is required")
	}
	if rule.ProviderID == "" {
		return errors.New("rule provider id is required")
	}
	if _, ok := r.providers[rule.ProviderID]; !ok {
		return fmt.Errorf("rule %s references missing provider %s", rule.ID, rule.ProviderID)
	}
	if rule.Priority < 0 {
		return errors.New("priority cannot be negative")
	}
	if rule.Weight < 0 {
		return errors.New("weight cannot be negative")
	}
	if rule.ActiveFrom.After(rule.ActiveTo) && !rule.ActiveTo.IsZero() {
		return errors.New("active_from cannot be after active_to")
	}
	if rule.TenantID == "" && (rule.UserID != "" || rule.CampaignID != "") {
		return errors.New("user/campaign scoped route requires tenant")
	}
	return nil
}
func prefixesOverlap(a, b []string) bool {
	if len(a) == 0 || len(b) == 0 {
		return false
	}
	for _, x := range a {
		for _, y := range b {
			if x != "" && y != "" && (strings.HasPrefix(x, y) || strings.HasPrefix(y, x)) {
				return true
			}
		}
	}
	return false
}

func routeSignature(rule Rule) string {
	p := strings.Join(rulePrefixes(rule), ",")
	mt := strings.Join(rule.MessageTypes, ",")
	return strings.Join([]string{rule.TenantID, rule.UserID, rule.CampaignID, rule.Sender, rule.Country, rule.Operator, rule.MCC, rule.MNC, p, mt, strconv.Itoa(rule.Priority)}, "|")
}

func (r *Router) Plan(ctx context.Context, req message.SubmitRequest, opts ...PlanOptions) (RoutePlan, error) {
	po := PlanOptions{Mode: RouteInitial, DryRun: true}
	if len(opts) > 0 {
		po = opts[0]
		if po.Mode == "" {
			po.Mode = RouteInitial
		}
	}
	if po.Now.IsZero() {
		po.Now = time.Now()
	}
	req = normalizeSubmitRequest(req)
	r.mu.RLock()
	defer r.mu.RUnlock()
	candidates, rejected := r.planLocked(req)
	candidates, rejected = r.applyProductionFiltersLocked(ctx, req, po, candidates, rejected, false)
	plan := RoutePlan{Request: req, Mode: po.Mode, Rejected: rejected, GeneratedAt: po.Now}
	for _, c := range candidates {
		plan.Candidates = append(plan.Candidates, CandidateExplanation{ProviderID: c.Provider.ID, RuleID: c.Rule.ID, Eligible: true, Priority: c.Rule.Priority, Score: c.ScoreBreakdown, Capacity: capacityFrom(c.Stats, r.sessionForDecisionLocked(req, c))})
	}
	for _, rej := range rejected {
		plan.Candidates = append(plan.Candidates, rej)
	}
	if len(candidates) > 0 {
		chosen := r.selectLocked(req, candidates)
		ce := CandidateExplanation{ProviderID: chosen.Provider.ID, RuleID: chosen.Rule.ID, Eligible: true, Priority: chosen.Rule.Priority, Score: chosen.ScoreBreakdown, Capacity: capacityFrom(chosen.Stats, r.sessions[chosen.Provider.ID])}
		plan.Selected = &ce
		plan.Decision = &chosen
	}
	if plan.Decision == nil {
		return plan, errors.New("no eligible provider for tenant/destination")
	}
	return plan, nil
}
func (r *Router) Simulate(ctx context.Context, req message.SubmitRequest) (RoutePlan, error) {
	return r.Plan(ctx, req, PlanOptions{Mode: RouteInitial, DryRun: true})
}
func (r *Router) RouteWithMode(ctx context.Context, req message.SubmitRequest, po PlanOptions) (Decision, error) {
	if po.Mode == "" {
		po.Mode = RouteInitial
	}
	return r.routeProduction(ctx, req, po)
}

func (r *Router) routeProduction(ctx context.Context, req message.SubmitRequest, po PlanOptions) (Decision, error) {
	req = normalizeSubmitRequest(req)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.expireCooldownLocked()
	candidates, rejected := r.planLocked(req)
	candidates, rejected = r.applyProductionFiltersLocked(ctx, req, po, candidates, rejected, true)
	if len(candidates) == 0 {
		return Decision{Rejected: rejected, Candidates: len(rejected)}, errors.New("no eligible provider for tenant/destination")
	}
	for len(candidates) > 0 {
		chosen := r.selectLocked(req, candidates)
		if r.limiter != nil {
			if rr := r.reserveLocked(ctx, req, chosen); len(rr) > 0 {
				rejected = append(rejected, CandidateExplanation{ProviderID: chosen.Provider.ID, RuleID: chosen.Rule.ID, Priority: chosen.Rule.Priority, Eligible: false, RejectedBy: rr, Score: chosen.ScoreBreakdown, Capacity: capacityFrom(chosen.Stats, r.sessions[chosen.Provider.ID])})
				candidates = removeDecision(candidates, chosen.Provider.ID, chosen.Rule.ID)
				continue
			}
		}
		if e := extra(r); e.capacity != nil {
			res, rr := e.capacity.Reserve(ctx, req, chosen)
			if len(rr) > 0 {
				rejected = append(rejected, CandidateExplanation{ProviderID: chosen.Provider.ID, RuleID: chosen.Rule.ID, Priority: chosen.Rule.Priority, Eligible: false, RejectedBy: rr, Score: chosen.ScoreBreakdown, Capacity: capacityFrom(chosen.Stats, r.sessions[chosen.Provider.ID])})
				candidates = removeDecision(candidates, chosen.Provider.ID, chosen.Rule.ID)
				continue
			}
			if res.ID != "" {
				if chosen.Provider.Meta == nil {
					chosen.Provider.Meta = map[string]string{}
				}
				chosen.Provider.Meta["capacity_reservation_id"] = res.ID
			}
		}
		snap := RouteDecisionSnapshot{ID: fmt.Sprintf("route-%d", time.Now().UnixNano()), MessageID: string(po.MessageID), RouteVersion: r.routeVersion, RuleID: chosen.Rule.ID, ProviderID: chosen.Provider.ID, TenantID: req.TenantID, Objective: objectiveFor(chosen.Rule, r.opts), Strategy: r.opts.Strategy, Score: chosen.Score, ScoreBreakdown: chosen.ScoreBreakdown, Rejected: rejected, CreatedAt: time.Now()}
		chosen.Rejected = rejected
		chosen.Candidates = len(candidates) + len(rejected)
		chosen.Snapshot = snap
		chosen.Reason = "eligible route selected"
		r.snapshots = append(r.snapshots, snap)
		_ = extra(r).snapStore.SaveSnapshot(ctx, snap)
		if r.opts.StickyBy != "" {
			if key := stickyKey(req, r.opts.StickyBy); key != "" {
				r.sticky[key] = chosen.Provider.ID
			}
		}
		return chosen, nil
	}
	return Decision{Rejected: rejected, Candidates: len(rejected)}, errors.New("all eligible routes rejected by capacity or rate limits")
}

func (r *Router) applyProductionFiltersLocked(ctx context.Context, req message.SubmitRequest, po PlanOptions, candidates []Decision, rejected []CandidateExplanation, reserve bool) ([]Decision, []CandidateExplanation) {
	e := extra(r)
	out := candidates[:0]
	for _, c := range candidates {
		reasons := []RejectionReason{}
		if po.ExcludeProviderID != "" && c.Provider.ID == po.ExcludeProviderID {
			reasons = append(reasons, RejectRuleMismatch)
		}
		if po.Mode == RouteRetry && po.AcceptedByProvider && po.OriginalProviderID != "" && c.Provider.ID != po.OriginalProviderID {
			reasons = append(reasons, RejectRuleMismatch)
		}
		if req.TenantID != "" && c.Rule.TenantID == "" && !e.allowGlobalRulesForTenants {
			reasons = append(reasons, RejectGlobalRouteDenied)
		}
		if e.requireActiveSession {
			s, ok := r.sessionForProviderOKLocked(req, c.Provider.ID)
			if !ok || !s.Healthy || s.Draining || strings.ToLower(s.State) != "bound" {
				reasons = append(reasons, RejectNoBind)
			}
		}
		if e.circuit != nil && !e.circuit.Allow(circuitKey(c.Provider.ID, req)) {
			reasons = append(reasons, RejectCircuitOpen)
		}
		if e.coverage != nil && !e.coverage.Allows(c.Provider, req) {
			reasons = append(reasons, RejectCoverage)
		}
		if e.compliance != nil {
			dec := e.compliance.Evaluate(ctx, req, firstNonEmpty(c.Rule.Country, req.Meta["country"]))
			if !dec.Allowed {
				reasons = append(reasons, RejectCompliance)
			}
		}
		if e.rating != nil {
			rate, err := e.rating.RateRoute(ctx, req, c.Provider.ID, estimateSegments(req))
			if err != nil {
				reasons = append(reasons, RejectRating)
			} else {
				if c.Rule.MaxCost > 0 && float64(rate.SellMicros)/1_000_000 > c.Rule.MaxCost {
					reasons = append(reasons, RejectCost)
				}
				if e.balance != nil {
					if err := e.balance.CanReserve(ctx, req.TenantID, rate); err != nil {
						reasons = append(reasons, RejectBalance)
					}
				} // Override cost score with rated sell price.
				c.Stats.CostPerSegment = float64(rate.SellMicros) / 1_000_000 / float64(max(1, estimateSegments(req)))
				c.ScoreBreakdown = scoreBreakdown(c.Rule, c.Provider, c.Stats, req, objectiveFor(c.Rule, r.opts), r.opts.Strategy)
				c.Score = c.ScoreBreakdown.Total
			}
		}
		if e.sla != nil {
			if sla, ok := e.sla.Lookup(c.Provider.ID, req); ok {
				if c.Rule.MinDeliveryRate > 0 && sla.DeliveryRate < c.Rule.MinDeliveryRate {
					reasons = append(reasons, RejectQuality)
				}
				if c.Rule.MaxLatency > 0 && sla.P95Latency > c.Rule.MaxLatency {
					reasons = append(reasons, RejectQuality)
				}
				c.Stats.DeliveryRate = sla.DeliveryRate
				c.Stats.AvgLatency = sla.P95Latency
				c.ScoreBreakdown = scoreBreakdown(c.Rule, c.Provider, c.Stats, req, objectiveFor(c.Rule, r.opts), r.opts.Strategy)
				c.Score = c.ScoreBreakdown.Total
			}
		}
		if e.capacity != nil {
			reasons = append(reasons, e.capacity.Check(ctx, req, c)...)
		}
		if len(reasons) > 0 {
			rejected = append(rejected, CandidateExplanation{ProviderID: c.Provider.ID, RuleID: c.Rule.ID, Priority: c.Rule.Priority, Eligible: false, RejectedBy: uniqueReasons(reasons), Score: c.ScoreBreakdown, Capacity: capacityFrom(c.Stats, r.sessionForDecisionLocked(req, c))})
			continue
		}
		out = append(out, c)
	}
	return out, rejected
}
func capacityBindKey(req message.SubmitRequest, d Decision) string {
	if req.Meta != nil {
		if k := req.Meta["bind_key"]; k != "" {
			return k
		}
		if sid := req.Meta["system_id"]; sid != "" {
			return d.Provider.ID + "|" + req.TenantID + "|" + sid + "|" + req.Meta["bind_mode"]
		}
	}
	return d.Provider.ID + "|" + req.TenantID
}

func circuitKey(pid string, req message.SubmitRequest) string {
	return pid + "|" + strings.ToUpper(req.Meta["country"]) + "|" + strings.ToUpper(req.Meta["operator"]) + "|" + strings.ToLower(req.Meta["message_type"])
}
func firstNonEmpty(xs ...string) string {
	for _, x := range xs {
		if strings.TrimSpace(x) != "" {
			return x
		}
	}
	return ""
}
func estimateSegments(req message.SubmitRequest) int {
	if req.Meta != nil {
		if n, err := strconv.Atoi(req.Meta["segments"]); err == nil && n > 0 {
			return n
		}
	}
	l := len([]rune(req.Text))
	if l == 0 {
		return 1
	}
	if l <= 160 {
		return 1
	}
	return (l + 152) / 153
}
func hasLetter(s string) bool {
	for _, r := range s {
		if (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') {
			return true
		}
	}
	return false
}

// PrefixTrie supports scalable all-prefix matching. The Router rebuilds and uses it automatically.
type PrefixTrie struct {
	children map[byte]*PrefixTrie
	rules    []int
}

func NewPrefixTrie() *PrefixTrie { return &PrefixTrie{children: map[byte]*PrefixTrie{}} }
func (t *PrefixTrie) Insert(prefix string, idx int) {
	if t == nil || prefix == "" {
		return
	}
	n := t
	for i := 0; i < len(prefix); i++ {
		b := prefix[i]
		if n.children == nil {
			n.children = map[byte]*PrefixTrie{}
		}
		if n.children[b] == nil {
			n.children[b] = NewPrefixTrie()
		}
		n = n.children[b]
	}
	n.rules = append(n.rules, idx)
}
func (t *PrefixTrie) MatchAll(number string) []int {
	if t == nil {
		return nil
	}
	out := []int{}
	n := t
	for i := 0; i < len(number); i++ {
		if len(n.rules) > 0 {
			out = append(out, n.rules...)
		}
		nxt := n.children[number[i]]
		if nxt == nil {
			return out
		}
		n = nxt
	}
	if len(n.rules) > 0 {
		out = append(out, n.rules...)
	}
	return out
}

// LoadBCLRoutes parses a small route-focused BCL-like format used by deployment configs.
// It intentionally accepts simple `key value` lines inside route/provider blocks so it works without mandatory third-party parser coupling.
func (r *Router) LoadBCLRoutes(data []byte) error {
	rules, providers, err := ParseBCLRoutes(data)
	if err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, p := range providers {
		if p.ID == "" {
			continue
		}
		if p.TPS <= 0 {
			p.TPS = 100
		}
		if p.WindowSize <= 0 {
			p.WindowSize = 16
		}
		r.providers[p.ID] = p
		r.states[p.ID] = stateFromProfile(p)
		if _, ok := r.stats[p.ID]; !ok {
			r.stats[p.ID] = ProviderStats{ProviderID: p.ID, QualityScore: 80, DeliveryRate: .98, MaxTPS: p.TPS, UpdatedAt: time.Now()}
		}
	}
	for _, rule := range rules {
		if err := r.validateRuleLocked(rule); err != nil {
			return err
		}
		if rule.Priority == 0 {
			rule.Priority = 1000
		}
		if rule.Weight <= 0 {
			rule.Weight = 1
		}
		rule.Enabled = !rule.Disabled
		r.rules = append(r.rules, rule)
	}
	r.routeVersion++
	r.rebuildLocked()
	return nil
}
func ParseBCLRoutes(data []byte) ([]Rule, []provider.Profile, error) {
	lines := strings.Split(string(data), "\n")
	var rules []Rule
	var providers []provider.Profile
	var curRule *Rule
	var curProvider *provider.Profile
	for _, raw := range lines {
		line := strings.TrimSpace(raw)
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, "//") {
			continue
		}
		if strings.HasPrefix(line, "provider ") && strings.Contains(line, "{") && curRule == nil {
			id := extractQuoted(line)
			curProvider = &provider.Profile{ID: id, Name: id, TPS: 100, WindowSize: 16}
			continue
		}
		if strings.HasPrefix(line, "route ") && strings.Contains(line, "{") {
			id := extractQuoted(line)
			curRule = &Rule{ID: id, Name: id, Priority: 1000, Weight: 1, Objective: ObjectiveBalanced, Enabled: true}
			continue
		}
		if line == "}" {
			if curProvider != nil {
				providers = append(providers, *curProvider)
				curProvider = nil
				continue
			}
			if curRule != nil {
				rules = append(rules, *curRule)
				curRule = nil
				continue
			}
		}
		k, v := splitKV(line)
		if curProvider != nil {
			applyProviderKV(curProvider, k, v)
			continue
		}
		if curRule != nil {
			applyRuleKV(curRule, k, v)
			continue
		}
	}
	return rules, providers, nil
}
func splitKV(line string) (string, string) {
	line = strings.TrimSuffix(line, ";")
	fields := strings.Fields(line)
	if len(fields) == 0 {
		return "", ""
	}
	k := fields[0]
	v := strings.TrimSpace(strings.TrimPrefix(line, k))
	return strings.ToLower(k), strings.Trim(v, " `\"")
}
func extractQuoted(s string) string {
	if i := strings.Index(s, "\""); i >= 0 {
		if j := strings.Index(s[i+1:], "\""); j >= 0 {
			return s[i+1 : i+1+j]
		}
	}
	f := strings.Fields(s)
	if len(f) > 1 {
		return strings.Trim(f[1], "{ `\"")
	}
	return ""
}
func applyProviderKV(p *provider.Profile, k, v string) {
	switch k {
	case "addr":
		p.Addr = v
	case "system_id":
		p.SystemID = v
	case "password":
		p.Password = v
	case "tps":
		p.TPS = atoi(v, 100)
	case "window", "window_size":
		p.WindowSize = atoi(v, 16)
	case "state":
		if p.Meta == nil {
			p.Meta = map[string]string{}
		}
		p.Meta["state"] = v
	}
}
func applyRuleKV(r *Rule, k, v string) {
	switch k {
	case "tenant":
		r.TenantID = v
	case "user":
		r.UserID = v
	case "campaign":
		r.CampaignID = v
	case "sender":
		r.Sender = v
	case "country":
		r.Country = strings.ToUpper(v)
	case "operator":
		r.Operator = v
	case "mcc":
		r.MCC = v
	case "mnc":
		r.MNC = v
	case "prefix":
		r.Prefix = v
	case "provider":
		r.ProviderID = v
	case "priority":
		r.Priority = atoi(v, 1000)
	case "weight":
		r.Weight = atoi(v, 1)
	case "objective":
		r.Objective = RoutingObjective(v)
	case "message_type":
		r.MessageTypes = []string{v}
	case "disabled":
		r.Disabled = truthy(v)
	}
}
func atoi(s string, def int) int {
	n, err := strconv.Atoi(strings.TrimSpace(s))
	if err != nil {
		return def
	}
	return n
}

// StableHashProvider is useful for storage-backed sticky routing without mutable state.
func StableHashProvider(key string, providers []string, weights map[string]int) string {
	sort.Strings(providers)
	total := 0
	for _, p := range providers {
		total += max(1, weights[p])
	}
	if total == 0 || len(providers) == 0 {
		return ""
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	n := int(h.Sum32() % uint32(total))
	cur := 0
	for _, p := range providers {
		cur += max(1, weights[p])
		if n < cur {
			return p
		}
	}
	return providers[0]
}
