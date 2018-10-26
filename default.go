package buffon

import (
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
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

type DefaultOption struct {
	Transport    http.RoundTripper
	Timeout      time.Duration
	MaxTimeout   time.Duration
	FetchLatency func(n time.Duration, method, routePattern string, statusCode int)
	FetchLogger  func(n time.Duration, method, urlPath string, statusCode int, reqID string)
}

type DefaultExecutor struct {
	option   *DefaultOption
	builder  *defaultBuilder
	fetcher  *defaultFetcher
	finisher *defaultFinisher
}

func NewDefaultExecutor(s string, opt *DefaultOption) (*DefaultExecutor, error) {
	v, err := newDefaultBuilder(s, opt.Timeout, opt.MaxTimeout)
	if err != nil {
		return nil, err
	}

	return &DefaultExecutor{
		option:  opt,
		builder: v,
		fetcher: &defaultFetcher{
			FetchLatency: opt.FetchLatency,
			FetchLogger:  opt.FetchLogger,
		},
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
	if c.option.Transport == nil {
		return http.DefaultTransport
	}

	return c.option.Transport
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

func (p payload) Bytes() []byte {
	if p.Body == nil {
		return nil
	}

	b, _ := json.Marshal(p.Body)
	return b
}

type defaultBuilder struct {
	BaseURL        *url.URL
	DefaultTimeout time.Duration
	MaxTimeout     time.Duration
}

func newDefaultBuilder(baseURL string, n time.Duration, m time.Duration) (*defaultBuilder, error) {
	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, err
	}

	return &defaultBuilder{
		BaseURL:        u,
		DefaultTimeout: n,
		MaxTimeout:     m,
	}, nil
}

func (x *defaultBuilder) Build(r *http.Request) (map[string]*http.Request, error) {
	v := new(request)

	err := json.NewDecoder(r.Body).Decode(v)
	if err != nil {
		return nil, errMissedQuery
	}

	mr := make(map[string]*http.Request)

	for k, v := range v.Aggregate {
		req := x.cloneRequest(r, v)
		req.URL.Scheme = x.BaseURL.Scheme
		req.URL.Host = x.BaseURL.Host
		req.Host = x.BaseURL.Host
		req.Header.Set("X-Timeout", x.Timeout(v).String())

		mr[k] = req
	}

	return mr, nil
}

func (x *defaultBuilder) Timeout(p payload) time.Duration {
	if p.Timeout == 0 {
		return x.DefaultTimeout
	}

	n := time.Duration(p.Timeout) * time.Millisecond

	if n > x.MaxTimeout {
		return x.MaxTimeout
	}

	return n
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

	u, err := url.Parse(t.Path)
	if err != nil {
		req.URL.Path = t.Path
		req.Header.Set("X-Invalid", "true")
		return req
	}

	if u.Path == "" {
		u.Path = "/"
	}

	q := req.URL.Query()

	for k, v := range u.Query() {
		q[k] = v
	}

	req.URL = u
	req.URL.RawQuery = q.Encode()

	if req.URL.Host != "" {
		req.Header.Set("X-Invalid", "true")
		return req
	}

	for k := range req.Header {
		if strings.HasSuffix(k, "-Original") {
			s := strings.Replace(k, "-Original", "", 1)
			req.Header.Set(s, r.Header.Get(k))
			req.Header.Del(k)
		}
	}

	body := bytes.NewReader(t.Bytes())
	req.Body = ioutil.NopCloser(body)
	req.ContentLength = int64(body.Len())

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

type defaultFetcher struct {
	FetchLatency func(n time.Duration, method, routePattern string, statusCode int)
	FetchLogger  func(n time.Duration, method, urlPath string, statusCode int, reqID string)
}

func (x *defaultFetcher) Fetch(mr map[string]*http.Request, z http.RoundTripper) (map[string]*http.Response, error) {
	var wg sync.WaitGroup

	mu := &sync.Mutex{}
	ms := make(map[string]*http.Response)
	es := make(ErrorMulti)

	wg.Add(len(mr))

	for k, v := range mr {
		go func(s string, r *http.Request) {
			start := time.Now()
			res, err := x.fetch(r, z)

			mu.Lock()

			dur := time.Since(start)
			x.fetchLatency(dur, r, res)
			x.fetchLogger(dur, r, res)

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

func (x *defaultFetcher) fetchLatency(n time.Duration, r *http.Request, res *http.Response) {
	x.FetchLatency(n, r.Method, x.routePattern(res), x.statusCode(r, res))
}

func (x *defaultFetcher) fetchLogger(n time.Duration, r *http.Request, res *http.Response) {
	x.FetchLogger(n, r.Method, r.URL.Path, x.statusCode(r, res), r.Header.Get("X-Request-Id"))
}

func (x *defaultFetcher) statusCode(r *http.Request, res *http.Response) int {
	if res != nil {
		return res.StatusCode
	}

	return http.StatusBadGateway
}

func (x *defaultFetcher) routePattern(res *http.Response) string {
	if res != nil {
		return res.Header.Get("X-Route-Pattern")
	}

	return ""
}

func (x *defaultFetcher) fetch(r *http.Request, z http.RoundTripper) (*http.Response, error) {
	if r.Header.Get("X-Invalid") != "" {
		return x.localResponse(r)
	}

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

func (x *defaultFetcher) localResponse(r *http.Request) (*http.Response, error) {
	body := x.buildError(r, errors.New("Not Found")).Error()

	return &http.Response{
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		Status:        fmt.Sprintf("%03d %s", http.StatusNotFound, http.StatusText(http.StatusNotFound)),
		StatusCode:    http.StatusNotFound,
		Request:       r,
		Header:        make(http.Header),
		Body:          ioutil.NopCloser(strings.NewReader(body)),
		ContentLength: int64(len(body)),
	}, nil
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
	mu      *sync.Mutex
	Data    map[string]interface{} `json:"data"`
	Message map[string]string      `json:"message,omitempty"`
	Meta    map[string]interface{} `json:"meta"`
	Error   map[string][]Error     `json:"error"`
}

func (r *response) Add(k string, n *json.Node) {
	data := new(interface{})
	meta := make(map[string]interface{})
	errs := []Error{}

	r.mu.Lock()
	defer r.mu.Unlock()

	if err := n.Get("data").Unmarshal(&data); err == nil {
		r.Data[k] = data
	}

	if msg := n.Get("message").String(); msg != "" {
		r.Message[k] = msg
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
		Data:    make(map[string]interface{}),
		Message: make(map[string]string),
		Meta:    make(map[string]interface{}),
		Error:   make(map[string][]Error),
		mu:      &sync.Mutex{},
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
	w.WriteHeader(code)
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
		defer res.Body.Close()

		b, err := x.readBody(res)
		if err != nil {
			es[k] = err
			continue
		}

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

func (x *defaultFinisher) readBody(res *http.Response) ([]byte, error) {
	var rbc io.ReadCloser

	switch res.Header.Get("Content-Encoding") {
	case "gzip":
		gz, err := gzip.NewReader(res.Body)
		if err != nil {
			return nil, x.buildError(res, err.Error(), http.StatusInternalServerError)
		}

		rbc = gz
	default:
		rbc = res.Body
	}

	defer rbc.Close()

	b, err := ioutil.ReadAll(rbc)
	if err != nil {
		return nil, x.buildError(res, err.Error(), res.StatusCode)
	}

	return b, nil
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
