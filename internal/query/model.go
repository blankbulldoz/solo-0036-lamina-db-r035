package query

import "lamina/internal/datum"

type Predicate struct {
	Column string      `json:"column"`
	Op     string      `json:"op"`
	Value  datum.Value `json:"value"`
}
type Ordering struct {
	Column string `json:"column"`
	Desc   bool   `json:"desc"`
}
type Aggregate struct {
	Function string `json:"function"`
	Column   string `json:"column,omitempty"`
}
type Request struct {
	Relation    string      `json:"relation"`
	Columns     []string    `json:"columns,omitempty"`
	Where       []Predicate `json:"where,omitempty"`
	Order       []Ordering  `json:"order,omitempty"`
	Limit       int         `json:"limit,omitempty"`
	At          *uint64     `json:"at,omitempty"`
	Transaction string      `json:"transaction,omitempty"`
	Aggregate   *Aggregate  `json:"aggregate,omitempty"`
	ForceScan   bool        `json:"force_scan,omitempty"`
}
type Plan struct {
	Access     string `json:"access"`
	Index      string `json:"index,omitempty"`
	Candidates int    `json:"candidates"`
	Snapshot   uint64 `json:"snapshot"`
	Residuals  int    `json:"residuals"`
}

// Work is the measured store-side cost of one execution. It exists so that
// plan.candidates (a planning-time access-path fact) is never mistaken for
// the actual read volume: probes counts version chains visited, dead counts
// probes with no visible row at the snapshot, and version_steps counts the
// chain search iterations, which grow with retained history.
type Work struct {
	Probes       int `json:"probes"`
	Visible      int `json:"visible"`
	Dead         int `json:"dead"`
	VersionSteps int `json:"version_steps"`
}

// Basis marks what an observation was made against: the commit head, the
// retained-history floor, and the access-path generation (bumped by index
// creation and vacuum rebuilds). Two results are only directly comparable
// when their bases match; epoch resets when the process reopens.
type Basis struct {
	Sequence uint64 `json:"sequence"`
	Horizon  uint64 `json:"horizon"`
	Epoch    uint64 `json:"epoch"`
}
type Result struct {
	Rows      []datum.Row `json:"rows"`
	Plan      Plan        `json:"plan"`
	Work      Work        `json:"work"`
	Basis     Basis       `json:"basis"`
	Scanned   int         `json:"scanned"`
	Matched   int         `json:"matched"`
	Truncated bool        `json:"truncated"`
}
