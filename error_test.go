package buffon_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/bukalapak/buffon"
	"github.com/stretchr/testify/assert"
)

func TestError(t *testing.T) {
	assert.Equal(t, "error", buffon.Error{Message: "error"}.Error())
}

func TestErrorMulti(t *testing.T) {
	errs := make(buffon.ErrorMulti)
	errs["foo"] = errors.New("foo")
	errs["bar"] = errors.New("bar")

	assert.ElementsMatch(t, []string{"foo", "bar"}, strings.Split(errs.Error(), ","))
}
