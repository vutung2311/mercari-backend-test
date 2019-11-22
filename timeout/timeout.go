package timeout

import (
	"context"
	"errors"
	"time"
)

var errTimedOut = errors.New("timed out")

// DoOrElse will run doFn and wait, if doFn take longer than timeout then it will to timeoutFn
func DoOrElse(timeout time.Duration, doFn func(context.Context) error, timeoutFn func()) error {
	errChan := make(chan error)
	ctx, cancelFunc := context.WithTimeout(context.Background(), timeout)
	defer cancelFunc()
	go func() {
		errChan <- doFn(ctx)
		close(errChan)
	}()

	select {
	case err := <-errChan:
		return err
	case <-ctx.Done():
		timeoutFn()
	}

	return errTimedOut
}

func IsTimedOut(err error) bool {
	return err == errTimedOut
}
