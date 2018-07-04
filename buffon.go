package buffon

import (
	"net/http"
)

type Executor interface {
	Build(r *http.Request) (map[string]*http.Request, error)
	Fetch(mr map[string]*http.Request) (map[string]*http.Response, error)
	Finish(w http.ResponseWriter, mr map[string]*http.Response, err error)
	FinishErr(w http.ResponseWriter, code int, err error)
}

type Aggregator struct {
	C Executor
}

func NewAggregator(c Executor) *Aggregator {
	return &Aggregator{C: c}
}

func (a *Aggregator) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	mr, err := a.C.Build(r)
	if err != nil {
		a.C.FinishErr(w, http.StatusBadRequest, err)
		return
	}

	ms, es := a.C.Fetch(mr)
	a.C.Finish(w, ms, es)
}
