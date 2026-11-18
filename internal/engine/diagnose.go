package engine

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"lamina/internal/query"
	"lamina/internal/versions"
	"sort"
)

// Deep diagnosis is an opt-in, bounded alternative to the always-on query
// facts. A session probes a fixed work queue in small chunks, one short
// critical section per request, so business traffic keeps flowing between
// chunks. Every chunk re-checks the observation basis: any commit, vacuum or
// index change in between ends the session instead of letting old access-path
// work be combined with new catalog facts.
const (
	diagnosisMaxSessions = 8
	diagnosisMaxBudget   = 16384
	diagnosisMaxChunk    = 1024
	diagnosisHotKeys     = 16
)

type DiagnoseRequest struct {
	Relation string            `json:"relation"`
	Where    []query.Predicate `json:"where,omitempty"`
	At       *uint64           `json:"at,omitempty"`
	Compare  bool              `json:"compare,omitempty"`
	Budget   int               `json:"budget,omitempty"`
	Chunk    int               `json:"chunk,omitempty"`
}

// KeyEvidence is exact per-key evidence for the hottest probed keys. The set
// is bounded but not sampled: the top keys by version-chain cost are kept
// exactly, so extreme hotspots cannot slip through.
type KeyEvidence struct {
	Key         string `json:"key"`
	Depth       int    `json:"depth"`
	Steps       int    `json:"steps"`
	Visible     bool   `json:"visible"`
	Matched     bool   `json:"matched"`
	Reclaimable int    `json:"reclaimable"`
}

// PathEvidence is the measured part of a diagnosis: every field is work the
// probes actually performed on one access path.
type PathEvidence struct {
	Path        string         `json:"path"`
	Role        string         `json:"role"`
	Probes      int            `json:"probes"`
	Visible     int            `json:"visible"`
	Dead        int            `json:"dead"`
	Matched     int            `json:"matched"`
	Steps       int            `json:"version_steps"`
	Depths      map[string]int `json:"chain_depths"`
	Reclaimable int            `json:"reclaimable_versions"`
	Hot         []KeyEvidence  `json:"hot_keys"`
	Queued      int            `json:"queued"`
	Total       int            `json:"total"`
	Complete    bool           `json:"complete"`
}

// DiagnosisEstimate is the derived part: facts computed from the catalog,
// index and version store at the session basis without executing the work.
// The vacuum projection uses the same Reclaimable rule the real vacuum runs.
type DiagnosisEstimate struct {
	Candidates int              `json:"candidates"`
	Chains     int              `json:"chains"`
	Vacuum     VacuumProjection `json:"vacuum"`
}
type VacuumProjection struct {
	Horizon uint64 `json:"horizon"`
	Pinned  bool   `json:"pinned"`
}

// Unattributable counts facts that are measured but cannot be traced to a
// cause with what the database retains: the index keeps no provenance for
// historical candidates, and a mixed basis makes cross-observation
// differences unattributable by construction.
type Unattributable struct {
	DeadCandidates int    `json:"dead_candidates"`
	Note           string `json:"note"`
}

type Recommendation struct {
	Action  string      `json:"action"`
	Basis   query.Basis `json:"basis"`
	Detail  string      `json:"detail"`
	Horizon uint64      `json:"horizon,omitempty"`
}

type Diagnosis struct {
	ID                    string            `json:"id"`
	Status                string            `json:"status"`
	Relation              string            `json:"relation"`
	Snapshot              uint64            `json:"snapshot"`
	Basis                 query.Basis       `json:"basis"`
	CurrentBasis          *query.Basis      `json:"current_basis,omitempty"`
	Probed                int               `json:"probed"`
	Queued                int               `json:"queued"`
	Measured              []*PathEvidence   `json:"measured"`
	Estimated             DiagnosisEstimate `json:"estimated"`
	Unattributable        Unattributable    `json:"unattributable"`
	Recommendation        *Recommendation   `json:"recommendation,omitempty"`
	RecommendationBlocked string            `json:"recommendation_blocked,omitempty"`

	where         []query.Predicate
	queue         []probeItem
	cursor        int
	chunk         int
	truncated     bool
	vacuumHorizon uint64
}
type probeItem struct {
	path int
	key  string
}

func (d *DB) Diagnose(req DiagnoseRequest) (Diagnosis, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(); err != nil {
		return Diagnosis{}, err
	}
	if len(d.diagnoses) >= diagnosisMaxSessions {
		return Diagnosis{}, fault("capacity", "8 active diagnoses reached; close finished sessions")
	}
	r, err := d.catalog.Relation(req.Relation)
	if err != nil {
		return Diagnosis{}, invalid(err)
	}
	at := d.sequence
	if req.At != nil {
		at = *req.At
	}
	if at < d.horizon || at > d.sequence {
		return Diagnosis{}, fault("snapshot", "snapshot unavailable; retained interval %d..%d", d.horizon, d.sequence)
	}
	if at < r.Created {
		return Diagnosis{}, fault("snapshot", "relation did not exist at snapshot")
	}
	if err = query.Bind(r, query.Request{Relation: req.Relation, Where: req.Where}); err != nil {
		return Diagnosis{}, invalid(err)
	}
	budget := req.Budget
	if budget == 0 {
		budget = 2048
	}
	if budget < 0 || budget > diagnosisMaxBudget {
		return Diagnosis{}, fault("invalid", "budget must be 0..16384 probes")
	}
	chunk := req.Chunk
	if chunk == 0 {
		chunk = 256
	}
	if chunk < 0 || chunk > diagnosisMaxChunk {
		return Diagnosis{}, fault("invalid", "chunk must be 0..1024 probes")
	}
	q := query.Request{Relation: req.Relation, Where: req.Where}
	chains := len(d.store.Tables[req.Relation])
	plan, candidates := query.Choose(d.catalog, d.indexes, q, at, chains)
	s := &Diagnosis{
		Status:   "probing",
		Relation: req.Relation,
		Snapshot: at,
		Basis:    d.basis(),
		where:    req.Where,
		chunk:    chunk,
		Measured: []*PathEvidence{},
		Estimated: DiagnosisEstimate{
			Candidates: plan.Candidates,
			Chains:     chains,
			Vacuum:     d.vacuumProjection(),
		},
		Unattributable: Unattributable{Note: "dead candidates are measured, but neither the index nor retained versions record which commit produced them"},
	}
	s.vacuumHorizon = s.Estimated.Vacuum.Horizon
	var id [16]byte
	if _, err = rand.Read(id[:]); err != nil {
		return Diagnosis{}, err
	}
	s.ID = hex.EncodeToString(id[:])
	chosen := &PathEvidence{Role: "chosen", Depths: map[string]int{}, Hot: []KeyEvidence{}}
	if plan.Access == "index" {
		chosen.Path = "index:" + plan.Index
	} else {
		chosen.Path = "scan"
	}
	s.Measured = append(s.Measured, chosen)
	var tableKeys []string
	if plan.Access != "index" || req.Compare {
		tableKeys = make([]string, 0, chains)
		for key := range d.store.Tables[req.Relation] {
			tableKeys = append(tableKeys, key)
		}
		sort.Strings(tableKeys)
	}
	queueKeys := func(path int, keys []string) {
		ev := s.Measured[path]
		ev.Total = len(keys)
		for _, key := range keys {
			if len(s.queue) >= budget {
				break
			}
			s.queue = append(s.queue, probeItem{path: path, key: key})
			ev.Queued++
		}
	}
	if plan.Access == "index" {
		queueKeys(0, candidates)
	} else {
		queueKeys(0, tableKeys)
	}
	if req.Compare && plan.Access == "index" {
		s.Measured = append(s.Measured, &PathEvidence{Path: "scan", Role: "comparison", Depths: map[string]int{}, Hot: []KeyEvidence{}})
		queueKeys(1, tableKeys)
	}
	for _, ev := range s.Measured {
		if ev.Queued < ev.Total {
			s.truncated = true
		}
	}
	s.Queued = len(s.queue)
	d.diagnoses[s.ID] = s
	d.probeChunk(s)
	return s.report(), nil
}

func (d *DB) ProbeDiagnosis(id string) (Diagnosis, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(); err != nil {
		return Diagnosis{}, err
	}
	s, ok := d.diagnoses[id]
	if !ok {
		return Diagnosis{}, fault("not_found", "diagnosis not active")
	}
	if s.Status == "probing" {
		d.probeChunk(s)
	}
	return s.report(), nil
}

func (d *DB) CloseDiagnosis(id string) (Diagnosis, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(); err != nil {
		return Diagnosis{}, err
	}
	s, ok := d.diagnoses[id]
	if !ok {
		return Diagnosis{}, fault("not_found", "diagnosis not active")
	}
	delete(d.diagnoses, id)
	if s.Status == "probing" {
		s.Status = "closed"
	}
	return s.report(), nil
}

// probeChunk runs one bounded critical section of probes. It refuses to
// continue across an observation-basis change: the work queue and the
// evidence so far belong to the session basis, and pairing them with a newer
// store or catalog would produce exactly the kind of mixed conclusion this
// diagnostic exists to prevent.
func (d *DB) probeChunk(s *Diagnosis) {
	if s.Snapshot < d.horizon {
		s.finalize("snapshot_lost", "snapshot was reclaimed by vacuum while diagnosing; evidence so far belongs to the stated basis and must not be combined with a newer one")
		return
	}
	if basis := d.basis(); basis != s.Basis {
		s.CurrentBasis = &basis
		s.finalize("mixed_basis", "commit or maintenance changed the observation basis mid-diagnosis; old access-path work is not paired with the new catalog")
		return
	}
	end := s.cursor + s.chunk
	if end > len(s.queue) {
		end = len(s.queue)
	}
	for ; s.cursor < end; s.cursor++ {
		item := s.queue[s.cursor]
		ev := s.Measured[item.path]
		probe := d.store.ProbeKey(s.Relation, item.key, s.Snapshot)
		reclaimable := versions.Reclaimable(d.store.Chain(s.Relation, item.key), s.vacuumHorizon)
		ev.Probes++
		ev.Steps += probe.Steps
		ev.Reclaimable += reclaimable
		ev.Depths[depthBucket(probe.Depth)]++
		matched := false
		if probe.Visible {
			ev.Visible++
			matched = query.Matches(probe.Row, s.where)
			if matched {
				ev.Matched++
			}
		} else {
			ev.Dead++
		}
		ev.considerHot(KeyEvidence{Key: item.key, Depth: probe.Depth, Steps: probe.Steps, Visible: probe.Visible, Matched: matched, Reclaimable: reclaimable})
	}
	s.Probed = s.cursor
	if s.cursor < len(s.queue) {
		return
	}
	for _, ev := range s.Measured {
		ev.Complete = ev.Probes == ev.Total
	}
	if s.truncated {
		s.finalize("budget_exhausted", "probe budget covered only part of the access path; rerun with a larger budget for a full-coverage recommendation")
		return
	}
	s.finalize("complete", "")
}

func (s *Diagnosis) finalize(status, blocked string) {
	s.Status = status
	dead := 0
	for _, ev := range s.Measured {
		dead += ev.Dead
	}
	s.Unattributable.DeadCandidates = dead
	if status != "complete" {
		s.RecommendationBlocked = blocked
		return
	}
	s.Recommendation = s.recommend()
}

// recommend runs only when every queued probe finished under one unchanged
// basis, so its action never rests on mixed or partial evidence.
func (s *Diagnosis) recommend() *Recommendation {
	chosen := s.Measured[0]
	est := s.Estimated
	r := &Recommendation{Action: "none", Basis: s.Basis}
	switch {
	case chosen.Reclaimable > 0 && (chosen.Dead > 0 || chosen.Reclaimable >= chosen.Probes):
		r.Action = "vacuum"
		r.Horizon = est.Vacuum.Horizon
		r.Detail = fmt.Sprintf("%d of %d probes hit dead or deep history and %d retained versions are reclaimable at horizon %d; rerun this diagnosis after vacuum to verify", chosen.Dead, chosen.Probes, chosen.Reclaimable, est.Vacuum.Horizon)
		if est.Vacuum.Pinned {
			r.Detail += "; horizon is pinned by an active transaction snapshot"
		}
	case chosen.Path != "scan" && est.Chains > 0 && chosen.Probes >= est.Chains:
		r.Action = "adjust_query"
		r.Detail = fmt.Sprintf("index candidates cover %d of %d table chains; the index is not selective for this filter and a plain scan probes less", chosen.Probes, est.Chains)
	case len(s.Measured) > 1 && s.Measured[1].Steps < chosen.Steps:
		r.Action = "adjust_query"
		r.Detail = fmt.Sprintf("comparison scan measured %d version steps against %d on the chosen index path; consider force_scan or a different predicate", s.Measured[1].Steps, chosen.Steps)
	case chosen.Dead > 0:
		r.Detail = fmt.Sprintf("%d dead candidates remain, but none are reclaimable at horizon %d (pinned or newer than the horizon); maintenance would not change this filter's cost now", chosen.Dead, est.Vacuum.Horizon)
	default:
		r.Detail = "measured cost is live visible rows; no maintenance is indicated, adjust the predicate or limit if the output is too large"
	}
	return r
}

// vacuumProjection computes the horizon a vacuum requested at the current
// head would actually reach, given the transactions active right now.
func (d *DB) vacuumProjection() VacuumProjection {
	horizon := d.sequence
	for _, tx := range d.transactions {
		if tx.Snapshot < horizon {
			horizon = tx.Snapshot
		}
	}
	return VacuumProjection{Horizon: horizon, Pinned: horizon < d.sequence}
}

func depthBucket(depth int) string {
	switch {
	case depth <= 1:
		return "1"
	case depth <= 4:
		return "2_4"
	case depth <= 16:
		return "5_16"
	case depth <= 64:
		return "17_64"
	default:
		return "65_plus"
	}
}

func hotter(a, b KeyEvidence) bool {
	if a.Steps != b.Steps {
		return a.Steps > b.Steps
	}
	if a.Depth != b.Depth {
		return a.Depth > b.Depth
	}
	return a.Key < b.Key
}

func (e *PathEvidence) considerHot(k KeyEvidence) {
	e.Hot = append(e.Hot, k)
	sort.Slice(e.Hot, func(i, j int) bool { return hotter(e.Hot[i], e.Hot[j]) })
	if len(e.Hot) > diagnosisHotKeys {
		e.Hot = e.Hot[:diagnosisHotKeys]
	}
}

// report deep-copies the session's public state so callers can marshal it
// after the lock is released without racing the next chunk.
func (s *Diagnosis) report() Diagnosis {
	out := *s
	out.Measured = make([]*PathEvidence, len(s.Measured))
	for i, ev := range s.Measured {
		c := *ev
		c.Depths = map[string]int{}
		for bucket, n := range ev.Depths {
			c.Depths[bucket] = n
		}
		c.Hot = append([]KeyEvidence{}, ev.Hot...)
		out.Measured[i] = &c
	}
	if s.CurrentBasis != nil {
		basis := *s.CurrentBasis
		out.CurrentBasis = &basis
	}
	return out
}
