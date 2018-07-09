package buffon_test

import (
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bmizerany/pat"
	"github.com/bukalapak/buffon"
	"github.com/bukalapak/ottoman/encoding/json"
	"github.com/stretchr/testify/assert"
)

func TestAggregator(t *testing.T) {
	backend := httptest.NewServer(handler())
	defer backend.Close()

	t.Run("queries", func(t *testing.T) {
		mtr := NewMetric()
		opt := &buffon.DefaultOption{
			Timeout:      time.Duration(1) * time.Second,
			FetchLatency: mtr.FetchLatency,
		}

		exc, err := buffon.NewDefaultExecutor(backend.URL, opt)
		assert.Nil(t, err)

		agg := buffon.NewAggregator(exc)

		matches, err := filepath.Glob("testdata/queries/*.json")
		assert.Nil(t, err)

		for _, match := range matches {
			t.Run(strings.Replace(match, "testdata/queries/", "", 1), func(t *testing.T) {
				file, err := os.Open(match)
				assert.Nil(t, err)
				defer file.Close()

				r := httptest.NewRequest("POST", "http://example.com/aggregate", file)
				r.Header.Set("X-Real-Ip", "202.212.202.212")
				r.Header.Set("X-Request-Id", "3a772b45-c5a3-4f7f-922e-372f216056c5")
				r.Header.Set("User-Agent", "gateway")
				r.Header.Set("User-Agent-Original", "aggregator")
				r.Header.Set("Accept-Encoding", "gzip")

				w := httptest.NewRecorder()

				agg.ServeHTTP(w, r)

				resFile := strings.Replace(match, "queries", "responses", 1)
				expected, err := ioutil.ReadFile(resFile)
				assert.Nil(t, err)
				assert.Equal(t, http.StatusOK, w.Code)
				assert.JSONEq(t, string(expected), w.Body.String())
			})
		}

		for k, v := range mtr.Data {
			assert.NotEmpty(t, k)
			assert.NotZero(t, v.Duration)
		}
	})

	t.Run("fetch-failure", func(t *testing.T) {
		opt := &buffon.DefaultOption{
			Transport:    &FailureTransport{},
			FetchLatency: NoopFetchLatency,
		}

		exc, err := buffon.NewDefaultExecutor(backend.URL, opt)
		assert.Nil(t, err)

		agg := buffon.NewAggregator(exc)

		s := strings.NewReader(`{"aggregate":{"x1":{"path":"/foo"}}}`)
		r := httptest.NewRequest("POST", "http://example.com/aggregate", s)
		w := httptest.NewRecorder()

		agg.ServeHTTP(w, r)

		n := json.NewNode(w.Body)
		assert.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, `GET /foo: Connection failure`, n.Get("error").Get("x1").GetN(0).Get("message").String())
	})

	t.Run("fetch-failure-response-body", func(t *testing.T) {
		opt := &buffon.DefaultOption{
			Transport:    &FailureBodyTransport{},
			FetchLatency: NoopFetchLatency,
		}

		exc, err := buffon.NewDefaultExecutor(backend.URL, opt)
		assert.Nil(t, err)

		agg := buffon.NewAggregator(exc)

		s := strings.NewReader(`{"aggregate":{"x1":{"path":"/foo"}}}`)
		r := httptest.NewRequest("POST", "http://example.com/aggregate", s)
		w := httptest.NewRecorder()

		agg.ServeHTTP(w, r)

		n := json.NewNode(w.Body)
		assert.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, `GET /foo: Unable to read response body`, n.Get("error").Get("x1").GetN(0).Get("message").String())
	})
}

func handler() http.Handler {
	m := pat.New()

	m.Get("/users/:id", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := ioutil.ReadFile("testdata/fixtures/user.json")
		w.Header().Set("Content-Type", "application/json")
		w.Write(b)
	}))

	m.Patch("/users/:id", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		z, err := parseBody(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		b, _ := ioutil.ReadFile("testdata/fixtures/user.json")
		b = bytes.Replace(b, []byte("Bambang Brotoseno"), []byte(z["name"]), 1)
		w.Header().Set("Content-Type", "application/json")
		w.Write(b)
	}))

	m.Post("/posts", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		z, err := parseBody(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		writeData(w, map[string]string{
			"agent": r.Header.Get("User-Agent"),
			"hello": fmt.Sprintf("Hello %s!", z["name"]),
			"ip":    r.Header.Get("X-Real-Ip"),
		})
	}))

	m.Get("/422", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := ioutil.ReadFile("testdata/fixtures/error-422.json")
		w.WriteHeader(http.StatusUnprocessableEntity)
		w.Header().Set("Content-Type", "application/json")
		w.Write(b)
	}))

	m.Get("/timeout", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(500 * time.Millisecond)
	}))

	m.Get("/timeout-config", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `{"data":{"timeout":"`+r.Header.Get("X-Timeout")+`"},"meta":{"http_status":200}}`)
	}))

	m.Get("/query", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeData(w, map[string]string{"url": r.URL.String()})
	}))

	m.Get("/text", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		io.WriteString(w, "hello!")
	}))

	m.Get("/xml", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		io.WriteString(w, `<?xml version="1.0" encoding="UTF-8"?><hello>world</hello>`)
	}))

	m.Get("/gzip", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Encoding", "gzip")
		w.Header().Set("Content-Type", "application/json")

		z := gzip.NewWriter(w)
		z.Write([]byte(`{"data":{"hello":"gzip!"},"meta":{"http_status":200}}`))
		z.Close()
	}))

	m.Get("/gzip-invalid", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Encoding", "gzip")
		w.Header().Set("Content-Type", "application/json")

		io.WriteString(w, `{"data":{"hello":"gzip!"},"meta":{"http_status":200}}`)
	}))

	m.Get("/header", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeData(w, map[string]string{"x-request-id": r.Header.Get("X-Request-Id")})
	}))

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		routePattern := r.URL.Path

		if strings.HasPrefix(r.URL.Path, "/users") {
			routePattern = "/users/:id"
		}

		w.Header().Set("X-Route-Pattern", routePattern)
		m.ServeHTTP(w, r)
	})
}

func parseBody(r io.Reader) (map[string]string, error) {
	z, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}

	v := make(map[string]string)

	if err := json.Unmarshal(z, &v); err != nil {
		return nil, err
	}

	return v, nil

}

func wrapData(m map[string]string) ([]byte, error) {
	type Meta struct {
		StatusCode int `json:"http_status"`
	}

	x := struct {
		Data map[string]string `json:"data"`
		Meta Meta              `json:"meta"`
	}{
		Data: m,
		Meta: Meta{StatusCode: http.StatusOK},
	}

	return json.Marshal(x)
}

func writeData(w http.ResponseWriter, m map[string]string) {
	b, err := wrapData(m)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(b)
}

type FailureTransport struct{}

func (t *FailureTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	return nil, errors.New("Connection failure")
}

type FailureBodyTransport struct{}

func (t *FailureBodyTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	return &http.Response{Body: ioutil.NopCloser(t), Request: r}, nil
}

func (t *FailureBodyTransport) Read(b []byte) (n int, err error) {
	return 0, errors.New("Unable to read response body")
}

func NoopFetchLatency(n time.Duration, method, routePattern string, code int) {}

type MetricData struct {
	Duration   time.Duration
	Method     string
	StatusCode int
}

type Metric struct {
	mu   *sync.Mutex
	Data map[string]MetricData
}

func NewMetric() *Metric {
	return &Metric{
		mu:   &sync.Mutex{},
		Data: make(map[string]MetricData),
	}
}

func (m *Metric) FetchLatency(n time.Duration, method, routePattern string, code int) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.Data[routePattern] = MetricData{
		Duration:   n,
		Method:     method,
		StatusCode: code,
	}
}
