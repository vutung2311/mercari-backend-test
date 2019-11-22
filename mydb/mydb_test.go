package mydb_test

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	sqlMock "github.com/m-rec/b089ba2b99a074a3b3a529a79f5c09888faf8be9/mock"
	"github.com/m-rec/b089ba2b99a074a3b3a529a79f5c09888faf8be9/mydb"
)

type fields struct {
	master             mydb.BackendSQL
	replicaReadTimeout time.Duration
	healthCheckPeriod  time.Duration
	maxReadAttempt     uint32
	readReplicas       []mydb.BackendSQL
}

func TestDB_Close(t *testing.T) {
	masterReturnNoError := new(sqlMock.SQL)
	masterReturnNoError.On("Close").Return(nil)
	masterCloseErr := errors.New("master close error")
	masterReturnError := new(sqlMock.SQL)
	masterReturnError.On("Close").Return(masterCloseErr)

	slaveCloseErr := errors.New("slave close error")
	slaveReturnError := new(sqlMock.SQL)
	slaveReturnError.On("Close").Return(slaveCloseErr)

	tests := []struct {
		name         string
		fields       fields
		wantedErr    error
		checkErrFunc func(err error) bool
	}{
		{
			name: "master return error",
			fields: fields{
				master:            masterReturnNoError,
				healthCheckPeriod: time.Second,
				readReplicas:      []mydb.BackendSQL{masterReturnNoError},
			},
			wantedErr:    nil,
			checkErrFunc: func(err error) bool { return err == nil },
		},
		{
			name: "master return error",
			fields: fields{
				master:            masterReturnError,
				healthCheckPeriod: time.Second,
				readReplicas:      []mydb.BackendSQL{masterReturnNoError},
			},
			wantedErr:    masterCloseErr,
			checkErrFunc: func(err error) bool { return strings.Contains(err.Error(), masterCloseErr.Error()) },
		},
		{
			name: "slave return error",
			fields: fields{
				master:            masterReturnNoError,
				healthCheckPeriod: time.Second,
				readReplicas:      []mydb.BackendSQL{slaveReturnError},
			},
			wantedErr:    slaveCloseErr,
			checkErrFunc: func(err error) bool { return strings.Contains(err.Error(), slaveCloseErr.Error()) },
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := mydb.NewDB(
				tt.fields.replicaReadTimeout,
				tt.fields.healthCheckPeriod,
				tt.fields.maxReadAttempt,
				tt.fields.master,
				tt.fields.readReplicas...,
			)
			err := db.Close()
			require.Truef(t, tt.checkErrFunc(err), "Close() error = %v, wantErr %v", err, tt.wantedErr)
		})
	}
}

func TestDB_PingContext(t *testing.T) {
	ctx := context.Background()
	ctxKey, ctx1Val := "key", "val1"
	ctx1 := context.WithValue(ctx, ctxKey, ctx1Val)
	ctx2Val := "val2"
	ctx2 := context.WithValue(ctx, ctxKey, ctx2Val)
	masterReturnNoError := new(sqlMock.SQL)
	masterReturnNoError.On(
		"PingContext",
		mock.MatchedBy(
			func(ctx context.Context) bool {
				return ctx.Value(ctxKey).(string) == ctx1Val || ctx.Value(ctxKey).(string) == ctx2Val
			},
		),
	).Return(nil)
	masterPingError := errors.New("master ping context error")
	masterReturnError := new(sqlMock.SQL)
	masterReturnError.On(
		"PingContext",
		mock.MatchedBy(
			func(ctx context.Context) bool {
				return ctx.Value(ctxKey).(string) == ctx2Val
			},
		),
	).Return(masterPingError)

	slavePingError := errors.New("slave ping context error")
	slaveReturnError := new(sqlMock.SQL)
	slaveReturnError.On(
		"PingContext",
		mock.MatchedBy(
			func(ctx context.Context) bool {
				return ctx.Value(ctxKey).(string) == ctx1Val
			},
		),
	).Return(nil).On(
		"PingContext",
		mock.MatchedBy(
			func(ctx context.Context) bool {
				return ctx.Value(ctxKey).(string) == ctx2Val
			},
		),
	).Return(slavePingError)

	type args struct {
		ctx context.Context
	}
	tests := []struct {
		name         string
		fields       fields
		args         args
		wantedErr    error
		checkErrFunc func(err error) bool
	}{
		{
			args: args{ctx1},
			name: "master return no error",
			fields: fields{
				master:            masterReturnNoError,
				healthCheckPeriod: time.Second,
				readReplicas:      []mydb.BackendSQL{masterReturnNoError},
			},
			wantedErr:    nil,
			checkErrFunc: func(err error) bool { return err == nil },
		},
		{
			args: args{ctx2},
			name: "master return error",
			fields: fields{
				master:            masterReturnError,
				healthCheckPeriod: time.Second,
				readReplicas:      []mydb.BackendSQL{masterReturnNoError},
			},
			wantedErr:    masterPingError,
			checkErrFunc: func(err error) bool { return strings.Contains(err.Error(), masterPingError.Error()) },
		},
		{
			args: args{ctx2},
			name: "slave return error",
			fields: fields{
				master:            masterReturnNoError,
				healthCheckPeriod: time.Second,
				readReplicas:      []mydb.BackendSQL{slaveReturnError},
			},
			wantedErr:    slavePingError,
			checkErrFunc: func(err error) bool { return strings.Contains(err.Error(), slavePingError.Error()) },
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := mydb.NewDB(
				tt.fields.replicaReadTimeout,
				tt.fields.healthCheckPeriod,
				tt.fields.maxReadAttempt,
				tt.fields.master,
				tt.fields.readReplicas...,
			)
			err := db.PingContext(tt.args.ctx)
			require.Truef(t, tt.checkErrFunc(err), "PingContext() error = %v, wantErr %v", err, tt.wantedErr)
		})
	}
}

func TestDB_QueryContext(t *testing.T) {
	replicaReadTimeout := 100 * time.Millisecond
	sleepLongerThanReplicaReadTimeout := func() {
		time.Sleep(replicaReadTimeout + replicaReadTimeout/2)
	}
	healthCheckPeriod := 500 * time.Millisecond
	sleepLongerThanHealthCheckPeriod := func() {
		time.Sleep(healthCheckPeriod + healthCheckPeriod/2)
	}
	maxReadAttempt := uint32(3)
	ctxKey := "context-key"

	updateCtx := context.WithValue(context.Background(), ctxKey, "updateQuery")
	var updateArgs = []interface{}{1, 2, 3}
	updateQuery := "UPDATE master SET write = true;"

	selectCtx := context.WithValue(context.Background(), ctxKey, "selectQuery")
	var selectArgs = []interface{}{4, 5, 6}
	selectQuery := "SELECT 1;"

	slaveTimedOutSelectCtx := context.WithValue(context.Background(), ctxKey, "slaveTimedOutSelect")
	var slaveTimedOutSelectArgs = []interface{}{7, 8, 9}
	slaveTimedOutSelectQuery := "SELECT * FROM slave1 LEFT JOIN slave2 ON slave1.id = slave2.id WHERE slave1.timeout = true;"

	firstTimeoutCtx := context.WithValue(context.Background(), ctxKey, "firstTimeout")
	var firstTimeoutArgs = []interface{}{7, 8, 9}
	firstTimeoutSelectQuery := "SELECT * FROM slave1 WHERE first_timeout = true;"
	secondTimeoutCtx := context.WithValue(context.Background(), ctxKey, "secondTimeout")
	var secondTimeoutArgs = []interface{}{10, 11, 12}
	secondTimeoutSelectQuery := "SELECT * FROM slave1 WHERE second_timeout = true;"

	master := new(sqlMock.SQL)
	masterResultRow := new(sql.Rows)
	master.On("Ping").Return(nil).
		On("QueryContext", updateCtx, updateQuery, updateArgs).Once().Return(masterResultRow, nil)

	replica1 := new(sqlMock.SQL)
	replica1ResultRow := new(sql.Rows)
	replica1.On("Ping").Return(nil).
		On("QueryContext", slaveTimedOutSelectCtx, slaveTimedOutSelectQuery, slaveTimedOutSelectArgs).Twice().Return(replica1ResultRow, nil).
		On("QueryContext", firstTimeoutCtx, firstTimeoutSelectQuery, firstTimeoutArgs).Once().Return(replica1ResultRow, nil).
		On("QueryContext", secondTimeoutCtx, secondTimeoutSelectQuery, secondTimeoutArgs).Once().Return(replica1ResultRow, nil)

	replica2ResultRow := new(sql.Rows)
	replica2 := new(sqlMock.SQL)
	replica2.On("Ping").Return(nil).
		On("QueryContext", selectCtx, selectQuery, selectArgs).Once().Return(replica2ResultRow, nil).
		On("QueryContext", secondTimeoutCtx, secondTimeoutSelectQuery, secondTimeoutArgs).Once().Return(replica2ResultRow, nil).
		On("QueryContext", slaveTimedOutSelectCtx, slaveTimedOutSelectQuery, slaveTimedOutSelectArgs).Once().Run(func(_ mock.Arguments) {
		sleepLongerThanReplicaReadTimeout()
	}).Return((*sql.Rows)(nil), errors.New("timeout error from replica2 for slaveTimedOutSelectCtx")).
		On("QueryContext", firstTimeoutCtx, firstTimeoutSelectQuery, firstTimeoutArgs).Once().Run(func(_ mock.Arguments) {
		sleepLongerThanReplicaReadTimeout()
	}).Return((*sql.Rows)(nil), errors.New("timeout error from replica2 for firstTimeoutCtx"))

	replica3 := new(sqlMock.SQL)
	replica3.On("Ping").Return(errors.New("offline")).
		On("QueryContext", slaveTimedOutSelectCtx, slaveTimedOutSelectQuery, slaveTimedOutSelectArgs).Once().Run(func(_ mock.Arguments) {
		sleepLongerThanReplicaReadTimeout()
	}).Return((*sql.Rows)(nil), errors.New("timeout error from replica3 for slaveTimedOutSelectCtx")).
		On("QueryContext", firstTimeoutCtx, firstTimeoutSelectQuery, firstTimeoutArgs).Once().Run(func(_ mock.Arguments) {
		sleepLongerThanReplicaReadTimeout()
	}).Return((*sql.Rows)(nil), errors.New("timeout error from replica3 for firstTimeoutCtx")).
		On("QueryContext", secondTimeoutCtx, secondTimeoutSelectQuery, secondTimeoutArgs).Once().Return(
		(*sql.Rows)(nil),
		errors.New("unexpected error from replica3 for secondTimeoutCtx"),
	)

	tests := []struct {
		name                     string
		fields                   fields
		testPerformAndAssertFunc func(*testing.T, *mydb.DB)
	}{
		{
			name: "write query to go master node",
			fields: fields{
				master:             master,
				replicaReadTimeout: replicaReadTimeout,
				healthCheckPeriod:  healthCheckPeriod,
				maxReadAttempt:     maxReadAttempt,
				readReplicas:       []mydb.BackendSQL{replica1, replica2, replica3},
			},
			testPerformAndAssertFunc: func(t *testing.T, db *mydb.DB) {
				got, err := db.QueryContext(updateCtx, updateQuery, updateArgs...)
				require.Same(t, masterResultRow, got)
				require.NoError(t, err)
			},
		},
		{
			name: "select query should go to next first online replica (replica2)",
			fields: fields{
				master:             master,
				replicaReadTimeout: replicaReadTimeout,
				healthCheckPeriod:  healthCheckPeriod,
				maxReadAttempt:     maxReadAttempt,
				readReplicas:       []mydb.BackendSQL{replica1, replica2, replica3},
			},
			testPerformAndAssertFunc: func(t *testing.T, db *mydb.DB) {
				got, err := db.QueryContext(selectCtx, selectQuery, selectArgs...)
				require.Same(t, replica2ResultRow, got)
				require.NoError(t, err)
			},
		},
		{
			name: "query should be retried if some replicas (replica2, replica3) is suddenly unreachable, replica statuses should be offline afterward",
			fields: fields{
				master:             master,
				replicaReadTimeout: replicaReadTimeout,
				healthCheckPeriod:  healthCheckPeriod,
				maxReadAttempt:     maxReadAttempt,
				readReplicas:       []mydb.BackendSQL{replica1, replica2, replica3},
			},
			testPerformAndAssertFunc: func(t *testing.T, db *mydb.DB) {
				got, err := db.QueryContext(slaveTimedOutSelectCtx, slaveTimedOutSelectQuery, slaveTimedOutSelectArgs...)
				require.Same(t, replica1ResultRow, got)
				require.NoError(t, err)
				got, err = db.QueryContext(slaveTimedOutSelectCtx, slaveTimedOutSelectQuery, slaveTimedOutSelectArgs...)
				require.Same(t, replica1ResultRow, got)
				require.NoError(t, err)
			},
		},
		{
			name: "health check should update replica statuses to online if they are back",
			fields: fields{
				master:             master,
				replicaReadTimeout: replicaReadTimeout,
				healthCheckPeriod:  healthCheckPeriod,
				maxReadAttempt:     maxReadAttempt,
				readReplicas:       []mydb.BackendSQL{replica1, replica2, replica3},
			},
			testPerformAndAssertFunc: func(t *testing.T, db *mydb.DB) {
				got, err := db.QueryContext(firstTimeoutCtx, firstTimeoutSelectQuery, firstTimeoutArgs...)
				require.Same(t, replica1ResultRow, got)
				require.NoError(t, err)
				got, err = db.QueryContext(secondTimeoutCtx, secondTimeoutSelectQuery, secondTimeoutArgs...)
				require.Same(t, replica1ResultRow, got)
				require.NoError(t, err)
				sleepLongerThanHealthCheckPeriod()
				got, err = db.QueryContext(secondTimeoutCtx, secondTimeoutSelectQuery, secondTimeoutArgs...)
				require.Same(t, replica2ResultRow, got)
				require.NoError(t, err)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := mydb.NewDB(
				tt.fields.replicaReadTimeout,
				tt.fields.healthCheckPeriod,
				tt.fields.maxReadAttempt,
				tt.fields.master,
				tt.fields.readReplicas...,
			)
			tt.testPerformAndAssertFunc(t, db)
		})
	}
}

func TestIsReadOnlyQuery(t *testing.T) {
	tests := []struct {
		name  string
		query string
		want  bool
	}{
		{
			name:  "SELECT query",
			query: "SELECT 1",
			want:  true,
		},
		{
			name:  "UPDATE query",
			query: "UPDATE foo FROM bar WHERE foo.bar_id = bar.id WHERE bar.name = 'foo'",
			want:  false,
		},
		{
			name: "UPDATE in CTE query",
			query: `WITH t AS (
    UPDATE products SET price = price * 1.05
    RETURNING *
)
SELECT * FROM t;
`,
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := mydb.IsReadOnlyQuery(tt.query)
			require.Equalf(t, tt.want, got, "IsReadOnlyQuery() got = %v, want %v", got, tt.want)
		})
	}
}
