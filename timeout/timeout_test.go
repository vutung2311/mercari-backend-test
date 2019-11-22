package timeout_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/m-rec/b089ba2b99a074a3b3a529a79f5c09888faf8be9/timeout"
)

func TestDoOrElse(t *testing.T) {
	err1 := errors.New("doFn first error")
	timedOut1 := false
	timedOut2 := false

	type args struct {
		timeout   time.Duration
		doFn      func(context.Context) error
		timeoutFn func()
	}
	tests := []struct {
		name    string
		args    args
		checkFn func(error) bool
		wantErr error
	}{
		{
			name: "doFn finish first",
			args: args{
				timeout: 10 * time.Millisecond,
				doFn: func(context.Context) error {
					<-time.After(5 * time.Millisecond)
					return err1
				},
				timeoutFn: func() {
					timedOut1 = true
				},
			},
			checkFn: func(err error) bool {
				return timedOut1 == false && err == err1
			},
		},
		{
			name: "doFn time out",
			args: args{
				timeout: 5 * time.Millisecond,
				doFn: func(context.Context) error {
					<-time.After(10 * time.Millisecond)
					return err1
				},
				timeoutFn: func() {
					timedOut2 = true
				},
			},
			checkFn: func(err error) bool {
				return timedOut2 == true && timeout.IsTimedOut(err)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := timeout.DoOrElse(tt.args.timeout, tt.args.doFn, tt.args.timeoutFn)
			if tt.checkFn != nil && !tt.checkFn(err) {
				t.Error("DoOrElse() checkFn false")
			}
		})
	}
}

func TestIsTimedOut(t *testing.T) {
	type args struct {
		err error
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := timeout.IsTimedOut(tt.args.err); got != tt.want {
				t.Errorf("IsTimedOut() = %v, want %v", got, tt.want)
			}
		})
	}
}
