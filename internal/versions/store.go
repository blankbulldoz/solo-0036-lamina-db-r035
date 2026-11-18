package versions

import "lamina/internal/datum"

type Mutation struct {
	Relation string    `json:"relation"`
	Key      string    `json:"key"`
	Row      datum.Row `json:"row,omitempty"`
	Deleted  bool      `json:"deleted,omitempty"`
}
type Version struct {
	Sequence uint64    `json:"sequence"`
	Row      datum.Row `json:"row,omitempty"`
	Deleted  bool      `json:"deleted,omitempty"`
}
type Store struct {
	Tables map[string]map[string][]Version `json:"tables"`
}

func New() *Store { return &Store{Tables: map[string]map[string][]Version{}} }
func (s *Store) Ensure(name string) {
	if s.Tables[name] == nil {
		s.Tables[name] = map[string][]Version{}
	}
}
func (s *Store) Apply(seq uint64, writes []Mutation) {
	for _, m := range writes {
		s.Ensure(m.Relation)
		s.Tables[m.Relation][m.Key] = append(s.Tables[m.Relation][m.Key], Version{Sequence: seq, Row: datum.Clone(m.Row), Deleted: m.Deleted})
	}
}
func Visible(chain []Version, at uint64) (Version, bool) {
	v, ok, _ := VisibleSteps(chain, at)
	return v, ok
}

// VisibleSteps is Visible plus the number of search iterations, so callers
// can report the measured cost of reaching a version instead of guessing it
// from candidate counts.
func VisibleSteps(chain []Version, at uint64) (Version, bool, int) {
	lo, hi := 0, len(chain)
	steps := 0
	for lo < hi {
		steps++
		mid := (lo + hi) / 2
		if chain[mid].Sequence <= at {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo == 0 {
		return Version{}, false, steps
	}
	v := chain[lo-1]
	return v, !v.Deleted, steps
}

// Probe is the measured fact of visiting one chain: how deep it is, how many
// search steps the visibility lookup took, and what was visible.
type Probe struct {
	Row     datum.Row
	Visible bool
	Steps   int
	Depth   int
}

func (s *Store) ProbeKey(table, key string, at uint64) Probe {
	chain := s.Tables[table][key]
	v, ok, steps := VisibleSteps(chain, at)
	return Probe{Row: datum.Clone(v.Row), Visible: ok, Steps: steps, Depth: len(chain)}
}

// Chain exposes the retained versions of one key for read-only inspection;
// callers must hold the database lock and must not mutate the result.
func (s *Store) Chain(table, key string) []Version { return s.Tables[table][key] }

// Walk aggregates the measured cost of visiting many chains.
type Walk struct {
	Probes  int
	Visible int
	Steps   int
}

func (s *Store) Latest(table, key string) uint64 {
	v := s.Tables[table][key]
	if len(v) == 0 {
		return 0
	}
	return v[len(v)-1].Sequence
}
func (s *Store) Scan(table string, at uint64) map[string]datum.Row {
	rows, _ := s.ScanWork(table, at)
	return rows
}
func (s *Store) ScanWork(table string, at uint64) (map[string]datum.Row, Walk) {
	out := map[string]datum.Row{}
	var w Walk
	for k, c := range s.Tables[table] {
		w.Probes++
		v, ok, steps := VisibleSteps(c, at)
		w.Steps += steps
		if ok {
			w.Visible++
			out[k] = datum.Clone(v.Row)
		}
	}
	return out, w
}
func (s *Store) Clone() *Store {
	out := New()
	for name, rows := range s.Tables {
		out.Ensure(name)
		for key, chain := range rows {
			for _, v := range chain {
				v.Row = datum.Clone(v.Row)
				out.Tables[name][key] = append(out.Tables[name][key], v)
			}
		}
	}
	return out
}
func (s *Store) Count() (keys, versions int) {
	for _, rows := range s.Tables {
		keys += len(rows)
		for _, v := range rows {
			versions += len(v)
		}
	}
	return
}
