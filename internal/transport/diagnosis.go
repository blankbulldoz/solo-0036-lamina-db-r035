package transport

import (
	"lamina/internal/engine"
	"net/http"
)

func (s *Server) diagnose(w http.ResponseWriter, r *http.Request) {
	var in engine.DiagnoseRequest
	if !decode(w, r, &in) {
		return
	}
	out, err := s.db.Diagnose(in)
	if err != nil {
		respondError(w, err)
		return
	}
	write(w, 201, out)
}
func (s *Server) probeDiagnosis(w http.ResponseWriter, r *http.Request) {
	out, err := s.db.ProbeDiagnosis(r.PathValue("id"))
	if err != nil {
		respondError(w, err)
		return
	}
	write(w, 200, out)
}
func (s *Server) closeDiagnosis(w http.ResponseWriter, r *http.Request) {
	out, err := s.db.CloseDiagnosis(r.PathValue("id"))
	if err != nil {
		respondError(w, err)
		return
	}
	write(w, 200, out)
}
