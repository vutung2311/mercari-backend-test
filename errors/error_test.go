package errors_test

import (
	"errors"
	"strings"
	"testing"

	stackError "github.com/m-rec/b089ba2b99a074a3b3a529a79f5c09888faf8be9/errors"
)

func TestWithStack(t *testing.T) {
	t.Run("with error", func(t *testing.T) {
		err := stackError.WithStack(errors.New("new error"))
		if err == nil || !strings.Contains(err.Error(), "error_test.go:13") {
			t.Fatal("wrong stack trace")
		}
	})
	t.Run("nil error", func(t *testing.T) {
		err := stackError.WithStack(nil)
		if err != nil {
			t.Fatal("wrong error")
		}
	})
}
