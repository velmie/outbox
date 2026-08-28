package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"time"
	"unicode/utf8"
)

const (
	maxNamedLockCharacters  = 64
	namedLockCleanupTimeout = 5 * time.Second
)

var (
	errNamedLockNameInvalid         = errors.New("outbox mysql: named lock name must contain 1 to 64 valid UTF-8 characters")
	errNamedLockAcquireNotConfirmed = errors.New("outbox mysql: named lock acquisition was not confirmed")
	errNamedLockReleaseNotConfirmed = errors.New("outbox mysql: named lock release was not confirmed")
)

func validateNamedLockName(name string) error {
	if name == "" || !utf8.ValidString(name) || utf8.RuneCountInString(name) > maxNamedLockCharacters {
		return errNamedLockNameInvalid
	}

	return nil
}

func tryNamedLock(ctx context.Context, conn *sql.Conn, name string) (bool, error) {
	var acquired sql.NullInt64
	if err := conn.QueryRowContext(ctx, "SELECT GET_LOCK(?, 0)", name).Scan(&acquired); err != nil {
		return false, fmt.Errorf("outbox mysql: acquire named lock failed: %w", err)
	}
	if !acquired.Valid {
		return false, errNamedLockAcquireNotConfirmed
	}
	switch acquired.Int64 {
	case 0:
		return false, nil
	case 1:
		return true, nil
	default:
		return false, errNamedLockAcquireNotConfirmed
	}
}

func releaseNamedLock(ctx context.Context, conn *sql.Conn, name string) error {
	releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), namedLockCleanupTimeout)
	defer cancel()

	var released sql.NullInt64
	if err := conn.QueryRowContext(releaseCtx, "SELECT RELEASE_LOCK(?)", name).Scan(&released); err != nil {
		discardConnection(conn)

		return fmt.Errorf("outbox mysql: release named lock failed: %w", err)
	}
	if err := validateNamedLockRelease(released); err != nil {
		discardConnection(conn)

		return err
	}

	return nil
}

func validateNamedLockRelease(result sql.NullInt64) error {
	if !result.Valid || result.Int64 != 1 {
		return errNamedLockReleaseNotConfirmed
	}

	return nil
}

func discardConnection(conn *sql.Conn) {
	_ = conn.Raw(func(any) error {
		return driver.ErrBadConn
	})
}
