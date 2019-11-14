package retry_test

import (
	"errors"
	"testing"

	"github.com/m-rec/b089ba2b99a074a3b3a529a79f5c09888faf8be9/retry"
)

func TestDo(t *testing.T) {
	err1 := errors.New("retry error")
	retryCount1 := 0
	retryCount2 := 0

	type args struct {
		fn         func() error
		retryTimes uint32
	}
	tests := []struct {
		name    string
		args    args
		checkFn func() bool
	}{
		{
			name: "retry 3 times",
			args: args{
				fn: func() error {
					retryCount1++
					return err1
				},
				retryTimes: 3,
			},
			checkFn: func() bool {
				return retryCount1 == 3
			},
		},
		{
			name: "try 1 time only",
			args: args{
				fn: func() error {
					retryCount2++
					return nil
				},
				retryTimes: 3,
			},
			checkFn: func() bool {
				return retryCount2 == 1
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			retry.Do(tt.args.fn, tt.args.retryTimes)
		})
	}
}
