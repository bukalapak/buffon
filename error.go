package buffon

import (
	"strings"
)

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
