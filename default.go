package buffon

import (
	"bytes"
	"errors"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/bukalapak/ottoman/encoding/json"
	httpclone "github.com/bukalapak/ottoman/http/clone"
)

var (
	errUnsupportedMedia = errors.New(http.StatusText(http.StatusUnsupportedMediaType))
	errMissedQuery      = errors.New("Must provide aggregate query")
)

type DefaultExecutor struct {
	Transport http.RoundTripper

	builder  *defaultBuilder
	fetcher  *defaultFetcher
	finisher *defaultFinisher
}

func NewDefaultExecutor(s string) (Executor, error) {
	v, err := newDefaultBuilder(s)
	if err != nil {
		return nil, err
	}

	return &DefaultExecutor{
		builder:  v,
		fetcher:  &defaultFetcher{},
		finisher: &defaultFinisher{},
	}, nil
}

func (c *DefaultExecutor) Build(r *http.Request) (map[string]*http.Request, error) {
	return c.builder.Build(r)
}

func (c *DefaultExecutor) Fetch(mr map[string]*http.Request) (map[string]*http.Response, error) {
	return c.fetcher.Fetch(mr, c.httpTransport())
}

func (c *DefaultExecutor) Finish(w http.ResponseWriter, ms map[string]*http.Response, err error) {
	c.finisher.Finish(w, ms, err)
}

func (c *DefaultExecutor) FinishErr(w http.ResponseWriter, code int, err error) {
	c.finisher.FinishErr(w, code, err)
}

func (c *DefaultExecutor) httpTransport() http.RoundTripper {
	if c.Transport == nil {
		return http.DefaultTransport
	}

	return c.Transport
}

type request struct {
	Aggregate map[string]payload `json:"aggregate"`
}

type payload struct {
	Method  string      `json:"method"`
	Path    string      `json:"path"`
	Body    interface{} `json:"body,omitempty"`
	Timeout int         `json:"timeout,omitempty"`
}

type defaultBuilder struct {
	BaseURL *url.URL
}

func newDefaultBuilder(baseURL string) (*defaultBuilder, error) {
	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, err
	}

	return &defaultBuilder{BaseURL: u}, nil
}

func (x *defaultBuilder) Build(r *http.Request) (map[string]*http.Request, error) {
	v := new(request)

	err := json.NewDecoder(r.Body).Decode(v)
	if err != nil {
		return nil, errMissedQuery
	}

	mr := make(map[string]*http.Request)

	for k, v := range v.Aggregate {
		dur := time.Duration(v.Timeout) * time.Millisecond
		req := x.cloneRequest(r, v)
		req.URL.Scheme = x.BaseURL.Scheme
		req.URL.Host = x.BaseURL.Host
		req.Header.Set("X-Timeout", dur.String())

		mr[k] = req
	}

	return mr, nil
}

func (x *defaultBuilder) httpMethod(t payload) string {
	if t.Method == "" {
		return "GET"
	}

	return t.Method
}

func (x *defaultBuilder) cloneRequest(r *http.Request, t payload) *http.Request {
	req := httpclone.Request(r)
	req.RequestURI = ""
	req.Method = x.httpMethod(t)

	u, _ := url.Parse(t.Path)
	req.URL = u

	for k := range req.Header {
		if strings.HasSuffix(k, "-Original") {
			s := strings.Replace(k, "-Original", "", 1)
			req.Header.Set(s, r.Header.Get(k))
			req.Header.Del(k)
		}
	}

	if b, err := json.Marshal(t.Body); err == nil {
		body := bytes.NewReader(b)
		req.Body = ioutil.NopCloser(body)
		req.ContentLength = int64(body.Len())
	}

	return req
}

type Error struct {
	Path       string `json:"-"`
	Method     string `json:"-"`
	Message    string `json:"message"`
	StatusCode int    `json:"-"`
	ErrCode    int    `json:"code"`
	ErrTimeout bool   `json:"-"`
}

func (err Error) Error() string {
	return err.Message
}

type ErrorMulti map[string]error

func (mrr ErrorMulti) Error() string {
	var ss []string

	for _, err := range mrr {
		ss = append(ss, err.Error())
	}

	return strings.Join(ss, ",")
}

type defaultFetcher struct{}

func (x *defaultFetcher) Fetch(mr map[string]*http.Request, z http.RoundTripper) (map[string]*http.Response, error) {
	var wg sync.WaitGroup

	mu := &sync.Mutex{}
	ms := make(map[string]*http.Response)
	es := make(ErrorMulti)

	wg.Add(len(mr))

	for k, v := range mr {
		go func(s string, r *http.Request) {
			res, err := x.fetch(r, z)

			mu.Lock()

			if err != nil {
				es[s] = x.buildError(r, err)
			} else {
				ms[s] = res
			}

			mu.Unlock()
			wg.Done()
		}(k, v)
	}

	wg.Wait()

	return ms, es
}

func (x *defaultFetcher) fetch(r *http.Request, z http.RoundTripper) (*http.Response, error) {
	var t time.Duration

	if n, err := time.ParseDuration(r.Header.Get("X-Timeout")); err == nil {
		t = n
	}

	htc := &http.Client{
		Timeout:   t,
		Transport: z,
	}

	return htc.Do(r)
}

func (x *defaultFetcher) buildError(req *http.Request, err error) error {
	message := err.Error()
	statusErrCode := http.StatusBadGateway

	var errTimeout bool

	if err, ok := err.(net.Error); ok {
		if err.Timeout() {
			errTimeout = true
			message = "timeout of " + req.Header.Get("X-Timeout") + " exceeded"
		} else {
			message = err.(*url.Error).Err.Error()
		}
	}

	return Error{
		Path:       req.URL.Path,
		Method:     req.Method,
		Message:    message,
		StatusCode: statusErrCode,
		ErrCode:    10000,
		ErrTimeout: errTimeout,
	}
}

type response struct {
	mu    *sync.Mutex
	Data  map[string]interface{} `json:"data"`
	Meta  map[string]interface{} `json:"meta"`
	Error map[string][]Error     `json:"error"`
}

func (r *response) Add(k string, n *json.Node) {
	data := make(map[string]interface{})
	meta := make(map[string]interface{})
	errs := []Error{}

	r.mu.Lock()
	defer r.mu.Unlock()

	if err := n.Get("data").Unmarshal(&data); err == nil {
		r.Data[k] = data
	}

	if err := n.Get("meta").Unmarshal(&meta); err == nil {
		r.Meta[k] = meta
	}

	if err := n.Get("errors").Unmarshal(&errs); err == nil {
		r.Error[k] = append(r.Error[k], errs...)
	}
}

func (r *response) AddError(k string, m Error) {
	r.mu.Lock()
	r.Error[k] = append(r.Error[k], m)
	r.mu.Unlock()
	r.addStatus(k, m.StatusCode)
}

func (r *response) addStatus(k string, code int) {
	type meta struct {
		StatusCode int `json:"http_status"`
	}

	r.mu.Lock()
	r.Meta[k] = meta{StatusCode: code}
	r.mu.Unlock()
}

func newResponse() *response {
	return &response{
		Data:  make(map[string]interface{}),
		Meta:  make(map[string]interface{}),
		Error: make(map[string][]Error),
		mu:    &sync.Mutex{},
	}
}

type defaultFinisher struct{}

func (x *defaultFinisher) Finish(w http.ResponseWriter, ms map[string]*http.Response, err error) {
	b := x.finish(ms, err.(ErrorMulti))
	w.Header().Set("Content-Type", "application/json")
	w.Write(b)
}

func (x *defaultFinisher) FinishErr(w http.ResponseWriter, code int, err error) {
	b := x.finishErr(code, err.Error())
	w.Header().Set("Content-Type", "application/json")
	w.Write(b)
}

func (x *defaultFinisher) finishErr(code int, message string) []byte {
	type Error struct {
		Message string `json:"message"`
	}

	type Meta struct {
		StatusCode int `json:"http_status"`
	}

	b, _ := json.Marshal(struct {
		Errors []Error `json:"errors"`
		Meta   Meta    `json:"meta"`
	}{
		Errors: []Error{{Message: message}},
		Meta:   Meta{StatusCode: code},
	})

	return b
}

func (x *defaultFinisher) finish(ms map[string]*http.Response, me ErrorMulti) []byte {
	ns, es := x.beforeFinish(ms)

	n := newResponse()

	for k, err := range me {
		n.AddError(k, x.wrapError(err))
	}

	for k, err := range es {
		n.AddError(k, x.wrapError(err))
	}

	for k, z := range ns {
		n.Add(k, z)
	}

	b, _ := json.Marshal(n)
	return b
}

func (x *defaultFinisher) beforeFinish(ms map[string]*http.Response) (map[string]*json.Node, ErrorMulti) {
	ns := make(map[string]*json.Node)
	es := make(ErrorMulti)

	for k, res := range ms {
		b, err := ioutil.ReadAll(res.Body)
		if err != nil {
			es[k] = x.buildError(res, err.Error(), res.StatusCode)
			continue
		}
		defer res.Body.Close()

		n := json.NewNode(bytes.NewReader(b))

		if x.hasErrorBody(n) {
			ns[k] = n
			continue
		}

		if res.StatusCode < http.StatusOK || res.StatusCode >= http.StatusMultipleChoices {
			es[k] = x.buildError(res, res.Status, res.StatusCode)
			continue
		}

		if !n.IsValid() {
			es[k] = x.buildError(res, errUnsupportedMedia.Error(), http.StatusUnsupportedMediaType)
			continue
		}

		ns[k] = n
	}

	return ns, es
}

func (x *defaultFinisher) hasErrorBody(n *json.Node) bool {
	return n.Get("errors").Len() > 0
}

func (x *defaultFinisher) buildError(res *http.Response, msg string, code int) error {
	err := Error{
		Path:       res.Request.URL.Path,
		Method:     res.Request.Method,
		ErrCode:    10000,
		StatusCode: code,
		Message:    msg,
	}

	return err
}

func (x *defaultFinisher) wrapError(err error) Error {
	er2 := err.(Error)
	erc := er2
	erc.Message = er2.Method + " " + er2.Path + ": " + er2.Message

	return erc
}