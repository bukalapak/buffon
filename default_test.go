package buffon_test

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/bukalapak/buffon"
	"github.com/stretchr/testify/assert"
)

func TestDefaultExecutor(t *testing.T) {
	t.Run("invalid-backend", func(t *testing.T) {
		opt := &buffon.DefaultOption{}
		exc, err := buffon.NewDefaultExecutor("http:// invalid", opt)
		assert.NotNil(t, err)
		assert.Nil(t, exc)
	})
}

func TestDefaultExecutor_MaxRequest(t *testing.T) {
	opt := &buffon.DefaultOption{
		MaxRequest: 1,
	}

	exc, err := buffon.NewDefaultExecutor("http://backend.dev", opt)
	assert.Nil(t, err)

	s := strings.NewReader(`{"aggregate":{"x1":{"path":"/foo"},"x2":{"path":"/bar"}}}`)
	r := httptest.NewRequest("POST", "http://example.com/aggregate", s)

	m, err := exc.Build(r)
	assert.Equal(t, "Too many aggregate requests", err.Error())
	assert.Nil(t, m)
}
