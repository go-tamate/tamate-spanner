package spanner

import (
	"context"

	"cloud.google.com/go/spanner"
	"github.com/Mitu217/tamate"
	"github.com/Mitu217/tamate/driver"
)

const driverName = "spanner"

type spannerDriver struct{}

func (sd *spannerDriver) Open(ctx context.Context, dsn string) (driver.Conn, error) {
	spannerClient, err := spanner.NewClient(ctx, dsn)
	if err != nil {
		return nil, err
	}
	sc := &spannerConn{
		DSN:           dsn,
		spannerClient: spannerClient,
	}
	return sc, nil
}

func init() {
	tamate.Register(driverName, &spannerDriver{})
}
