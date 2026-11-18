package versions

type VacuumResult struct {
	Horizon  uint64 `json:"horizon"`
	Removed  int    `json:"removed"`
	Retained int    `json:"retained"`
	Pinned   bool   `json:"pinned"`
}

// Reclaimable is the exact vacuum rule phrased as a projection: how many
// versions of this chain a vacuum at horizon would remove. Diagnostics use it
// to estimate what maintenance would do; Vacuum uses it to actually do it, so
// the estimate and the operation cannot drift apart.
func Reclaimable(chain []Version, horizon uint64) int {
	keep := 0
	for keep+1 < len(chain) && chain[keep+1].Sequence <= horizon {
		keep++
	}
	if len(chain) > 0 && chain[len(chain)-1].Sequence <= horizon && chain[len(chain)-1].Deleted {
		return len(chain)
	}
	return keep
}

func (s *Store) Vacuum(horizon uint64) VacuumResult {
	result := VacuumResult{Horizon: horizon}
	for _, rows := range s.Tables {
		for key, chain := range rows {
			reclaimed := Reclaimable(chain, horizon)
			result.Removed += reclaimed
			remaining := append([]Version(nil), chain[reclaimed:]...)
			if len(remaining) == 0 {
				delete(rows, key)
				continue
			}
			rows[key] = remaining
			result.Retained += len(remaining)
		}
	}
	return result
}
