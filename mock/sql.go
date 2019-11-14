package mock

import (
	"context"
	"database/sql"
	"time"

	"github.com/stretchr/testify/mock"
)

type SQL struct {
	mock.Mock
}

func (m *SQL) Begin() (*sql.Tx, error) {
	call := m.Called()
	return call.Get(0).(*sql.Tx), call.Error(1)
}

func (m *SQL) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	call := m.Called(ctx, opts)
	return call.Get(0).(*sql.Tx), call.Error(1)
}

func (m *SQL) Exec(query string, args ...interface{}) (sql.Result, error) {
	call := m.Called(query, args)
	return call.Get(0).(sql.Result), call.Error(1)
}

func (m *SQL) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	call := m.Called(ctx, query, args)
	return call.Get(0).(sql.Result), call.Error(1)
}

func (m *SQL) Prepare(query string) (*sql.Stmt, error) {
	call := m.Called(query)
	return call.Get(0).(*sql.Stmt), call.Error(1)
}

func (m *SQL) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	call := m.Called(ctx, query)
	return call.Get(0).(*sql.Stmt), call.Error(1)
}

func (m *SQL) Ping() error {
	return m.Called().Error(0)
}

func (m *SQL) PingContext(ctx context.Context) error {
	return m.Called(ctx).Error(0)
}

func (m *SQL) Query(query string, args ...interface{}) (*sql.Rows, error) {
	call := m.Called(query, args)
	return call.Get(0).(*sql.Rows), call.Error(1)
}

func (m *SQL) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	call := m.Called(ctx, query, args)
	return call.Get(0).(*sql.Rows), call.Error(1)
}

func (m *SQL) QueryRow(query string, args ...interface{}) *sql.Row {
	call := m.Called(query, args)
	return call.Get(0).(*sql.Row)
}

func (m *SQL) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	call := m.Called(ctx, query, args)
	return call.Get(0).(*sql.Row)

}

func (m *SQL) SetConnMaxLifetime(d time.Duration) {
	m.Called(d)
	return
}

func (m *SQL) SetMaxIdleConns(n int) {
	m.Called(n)
	return

}

func (m *SQL) SetMaxOpenConns(n int) {
	m.Called(n)
	return
}

func (m *SQL) Close() error {
	return m.Called().Error(0)
}
