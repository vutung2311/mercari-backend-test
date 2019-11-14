package timeout

import (
	"errors"
	"time"
)

var errTimedOut = errors.New("timed out")

// DoOrElse will run doFn and wait, if doFn take longer than timeout then it will to timeoutFn
func DoOrElse(timeout time.Duration, doFn func() error, timeoutFn func()) error {
	errChan := make(chan error)
	go func() {
		errChan <- doFn()
		close(errChan)
	}()

	select {
	case err := <-errChan:
		return err
	case <-time.After(timeout):
		timeoutFn()
	}

	return errTimedOut
}

func IsTimedOut(err error) bool {
	return err == errTimedOut
}
