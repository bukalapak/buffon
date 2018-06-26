package buffon_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bmizerany/pat"
	"github.com/bukalapak/buffon"
	"github.com/stretchr/testify/assert"
)

func TestAggregator(t *testing.T) {
	backend := httptest.NewServer(handler())
	defer backend.Close()

	rev, err := buffon.NewResolver(backend.URL)
	assert.Nil(t, err)

	agg := &buffon.Aggregator{
		Resolver: rev,
	}

	matches, err := filepath.Glob("testdata/queries/*.json")
	assert.Nil(t, err)

	for _, match := range matches {
		t.Run(strings.Replace(match, "testdata/", "", 1), func(t *testing.T) {
			file, err := os.Open(match)
			assert.Nil(t, err)
			defer file.Close()

			r := httptest.NewRequest("POST", "http://example.com/aggregate", file)
			r.Header.Set("X-Real-Ip", "202.212.202.212")
			r.Header.Set("User-Agent", "gateway")
			r.Header.Set("User-Agent-Original", "aggregator")

			w := httptest.NewRecorder()

			agg.ServeHTTP(w, r)

			resFile := strings.Replace(match, "queries", "responses", 1)
			expected, err := ioutil.ReadFile(resFile)
			assert.Nil(t, err)
			assert.JSONEq(t, string(expected), w.Body.String())
		})
	}
}

func TestResolver(t *testing.T) {
	t.Run("invalid-backend", func(t *testing.T) {
		rev, err := buffon.NewResolver("http:// invalid")
		assert.NotNil(t, err)
		assert.Nil(t, rev)
	})
}

func TestResponseError(t *testing.T) {
	err := buffon.ResponseError{
		ErrCode:    10000,
		StatusCode: http.StatusNotFound,
		Message:    "GET /unknown: 404 Not Found",
	}

	assert.Equal(t, "GET /unknown: 404 Not Found", err.Error())
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

	return m
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
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.Write(b)
}
