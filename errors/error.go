package errors

import (
	"fmt"
	"runtime"
)

type Error struct {
	stackTrace   []byte
	wrappedError error
}

// WithStack will wrap the provided error in another error with the stack-trace at the line of original returned error
func WithStack(err error) error {
	if err == nil {
		return err
	}
	stackTraceBuf := make([]byte, 512)
	runtime.Stack(stackTraceBuf, false)
	return Error{
		stackTrace:   stackTraceBuf,
		wrappedError: err,
	}
}

// Error implements interface of error
func (err Error) Error() string {
	return fmt.Sprintf(
		"err: %s\nstack-trace:%s",
		err.wrappedError.Error(),
		err.stackTrace,
	)
}
