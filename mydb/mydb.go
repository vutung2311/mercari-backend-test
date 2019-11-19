package mydb

import (
	"context"
	"database/sql"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	stackError "github.com/m-rec/b089ba2b99a074a3b3a529a79f5c09888faf8be9/errors"
	"github.com/m-rec/b089ba2b99a074a3b3a529a79f5c09888faf8be9/retry"
	"github.com/m-rec/b089ba2b99a074a3b3a529a79f5c09888faf8be9/timeout"

	"golang.org/x/sync/errgroup"
)

type BackendSQL interface {
	Begin() (*sql.Tx, error)
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
	Exec(query string, args ...interface{}) (sql.Result, error)
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	Prepare(query string) (*sql.Stmt, error)
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
	Ping() error
	PingContext(context.Context) error
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
	SetConnMaxLifetime(d time.Duration)
	SetMaxIdleConns(n int)
	SetMaxOpenConns(n int)
	Close() error
}

func forEachReplica(ctx context.Context, backendDBs []*readReplica, dbFunc func(context.Context, BackendSQL) error) error {
	g, ctx := errgroup.WithContext(ctx)
	for _, db := range backendDBs {
		db := db
		g.Go(func() error {
			return dbFunc(ctx, db)
		})
	}
	return g.Wait()
}

var openBracketRegex = regexp.MustCompile("(\\s*)?\\((\\s*)?")
var closeBracketRegex = regexp.MustCompile("(\\s*)?\\)(\\s*)?")
var commaRegex = regexp.MustCompile("(\\s*)?,(\\s*)?")
var semiColonRegex = regexp.MustCompile("(\\s*)?;(\\s*)?")

func IsReadOnlyQuery(query string) bool {
	tokensMap := make(map[string]bool)
	lineSlice := strings.Split(query, "\n")
	for i := range lineSlice {
		lineSlice[i] = openBracketRegex.ReplaceAllString(lineSlice[i], " ( ")
		lineSlice[i] = closeBracketRegex.ReplaceAllString(lineSlice[i], " ) ")
		lineSlice[i] = commaRegex.ReplaceAllString(lineSlice[i], " , ")
		lineSlice[i] = semiColonRegex.ReplaceAllString(lineSlice[i], " ; ")
		lineSlice[i] = strings.TrimSpace(lineSlice[i])
		for _, token := range strings.Split(lineSlice[i], " ") {
			tokensMap[strings.TrimSpace(token)] = true
		}
	}

	if tokensMap["CREATE"] || tokensMap["UPDATE"] || tokensMap["DELETE"] {
		return false
	}

	return true
}

type DB struct {
	master       *readReplica
	readReplicas []*readReplica

	maxReadAttempt     uint32
	readReplicaTimeout time.Duration
	healthCheckPeriod  time.Duration

	// This can be overflowed but it's fine
	currentCallCount uint32
	dbClosed         chan bool
}

func NewDB(
	replicaReadTimeout, healthCheckPeriod time.Duration,
	maxReadAttempt uint32,
	master BackendSQL,
	readReplicas ...BackendSQL,
) *DB {
	db := &DB{
		master:             &readReplica{BackendSQL: master, online: true},
		readReplicaTimeout: replicaReadTimeout,
		healthCheckPeriod:  healthCheckPeriod,
		maxReadAttempt:     maxReadAttempt,
		readReplicas:       make([]*readReplica, 0, len(readReplicas)),
		dbClosed:           make(chan bool),
	}
	for i := range readReplicas {
		if readReplicas[i] != nil {
			db.readReplicas = append(
				db.readReplicas,
				&readReplica{
					BackendSQL: readReplicas[i],
					online:     true,
					mutex:      new(sync.RWMutex),
				},
			)
		}
	}
	db.startPeriodHealthCheck()

	return db
}

func (db *DB) startPeriodHealthCheck() {
	for i := range db.readReplicas {
		i := i
		go func() {
			ticker := time.NewTicker(db.healthCheckPeriod)
			defer ticker.Stop()

			for {
				select {
				case <-db.dbClosed:
					return
				case <-ticker.C:
					if db.readReplicas[i].isOnline() {
						continue
					}
					_ = timeout.DoOrElse(
						db.readReplicaTimeout,
						func() error {
							if db.readReplicas[i].Ping() == nil {
								db.readReplicas[i].setOnline()
							}
							return nil
						},
						func() {},
					)
				}
			}
		}()
	}
}

func (db *DB) getAllBackend() []*readReplica {
	return append([]*readReplica{db.master}, db.readReplicas...)
}

func (db *DB) getNextReplicaIndex() uint32 {
	return atomic.AddUint32(&db.currentCallCount, 1) % db.getReplicaCount()
}

func (db *DB) getReplicaCount() uint32 {
	return uint32(len(db.readReplicas))
}

func (db *DB) getRoundRobinReplicaBackend(ctx context.Context, query string) *readReplica {
	if !IsReadOnlyQuery(query) {
		return db.master
	}
	for i := uint32(0); i < db.getReplicaCount(); i++ {
		nextIndex := db.getNextReplicaIndex()
		if db.readReplicas[nextIndex].isOnline() {
			return db.readReplicas[nextIndex]
		}
	}

	panic("all replicas are unreachable")
}

func (db *DB) retryOnReplicaElseTimeout(getReplicaFn func() *readReplica, doFn func(*readReplica) error) {
	retry.Do(
		func() error {
			replica := getReplicaFn()
			err := timeout.DoOrElse(
				db.readReplicaTimeout,
				func() error {
					return doFn(replica)
				},
				func() {
					replica.setOffline()
				},
			)
			if !timeout.IsTimedOut(err) {
				return nil
			}
			return err
		},
		db.maxReadAttempt,
	)
}

func (db *DB) Query(query string, args ...interface{}) (*sql.Rows, error) {
	var (
		rows *sql.Rows
		err  error
	)

	db.retryOnReplicaElseTimeout(
		func() *readReplica {
			return db.getRoundRobinReplicaBackend(context.Background(), query)
		},
		func(replica *readReplica) error {
			rows, err = replica.Query(query, args...)
			return err
		},
	)

	return rows, err
}

func (db *DB) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	var (
		rows *sql.Rows
		err  error
	)

	db.retryOnReplicaElseTimeout(
		func() *readReplica {
			return db.getRoundRobinReplicaBackend(context.Background(), query)
		},
		func(replica *readReplica) error {
			rows, err = replica.QueryContext(ctx, query, args...)
			return err
		},
	)

	return rows, err
}

func (db *DB) QueryRow(query string, args ...interface{}) *sql.Row {
	var row *sql.Row

	db.retryOnReplicaElseTimeout(
		func() *readReplica {
			return db.getRoundRobinReplicaBackend(context.Background(), query)
		},
		func(replica *readReplica) error {
			row = replica.QueryRow(query, args...)
			return nil
		},
	)

	return row
}

func (db *DB) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	var row *sql.Row

	db.retryOnReplicaElseTimeout(
		func() *readReplica {
			return db.getRoundRobinReplicaBackend(context.Background(), query)
		},
		func(replica *readReplica) error {
			row = replica.QueryRowContext(ctx, query, args...)
			return nil
		},
	)

	return row
}

func (db *DB) Begin() (*sql.Tx, error) {
	return db.master.Begin()
}

func (db *DB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	return db.master.BeginTx(ctx, opts)
}

func (db *DB) Exec(query string, args ...interface{}) (sql.Result, error) {
	return db.master.Exec(query, args...)
}

func (db *DB) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return db.master.ExecContext(ctx, query, args...)
}

func (db *DB) Prepare(query string) (*sql.Stmt, error) {
	return db.master.Prepare(query)
}

func (db *DB) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	return db.master.PrepareContext(ctx, query)
}

func (db *DB) Ping() error {
	return forEachReplica(
		context.Background(),
		db.getAllBackend(),
		func(_ context.Context, backendSQL BackendSQL) error {
			return stackError.WithStack(backendSQL.Ping())
		},
	)
}

func (db *DB) PingContext(ctx context.Context) error {
	return forEachReplica(
		ctx,
		db.getAllBackend(),
		func(ctx context.Context, backendSQL BackendSQL) error {
			return stackError.WithStack(backendSQL.PingContext(ctx))
		},
	)
}

func (db *DB) Close() error {
	return forEachReplica(
		context.Background(),
		db.getAllBackend(),
		func(_ context.Context, backendSQL BackendSQL) error {
			return stackError.WithStack(backendSQL.Close())
		},
	)
}

func (db *DB) SetConnMaxLifetime(d time.Duration) {
	_ = forEachReplica(
		context.Background(),
		db.getAllBackend(),
		func(_ context.Context, backendSQL BackendSQL) error {
			backendSQL.SetConnMaxLifetime(d)
			return nil
		},
	)
}

func (db *DB) SetMaxIdleConns(n int) {
	_ = forEachReplica(
		context.Background(),
		db.getAllBackend(),
		func(_ context.Context, backendSQL BackendSQL) error {
			backendSQL.SetMaxIdleConns(n)
			return nil
		},
	)
}

func (db *DB) SetMaxOpenConns(n int) {
	_ = forEachReplica(
		context.Background(),
		db.getAllBackend(),
		func(_ context.Context, backendSQL BackendSQL) error {
			backendSQL.SetMaxOpenConns(n)
			return nil
		},
	)
}
