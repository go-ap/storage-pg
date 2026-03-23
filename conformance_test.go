//go:build conformance

package pg

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	conformance "github.com/go-ap/storage-conformance-suite"
	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

func initStorage(t *testing.T) conformance.ActivityPubStorage {
	ctx := context.Background()

	pgContainer, err := postgres.Run(ctx, "postgres:18-alpine",
		postgres.WithInitScripts(filepath.Join("images", "init-db.sql")),
		postgres.WithDatabase("test-db"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("postgres"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).WithStartupTimeout(5*time.Second)),
	)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if err := pgContainer.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate pgContainer: %s", err)
		}
	})

	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("err getting connection string: %s", err)
	}

	pconf, err := pgx.ParseConfig(connStr)
	if err != nil {
		t.Fatalf("err getting config: %s", err)
	}

	conf := Config{
		Host:     pconf.Host,
		Port:     pconf.Port,
		Database: pconf.Database,
		User:     pconf.User,
		Password: pconf.Password,
		LogFn:    t.Logf,
		ErrFn:    t.Logf,
	}

	if err := Bootstrap(conf); err != nil {
		t.Fatalf("unable to bootstrap storage: %s", err)
	}
	t.Skipf("we're not ready yet")
	storage, err := New(conf)
	if err != nil {
		t.Fatalf("unable to initialize storage: %s", err)
	}

	return storage
}

func Test_Conformance(t *testing.T) {
	conformance.Suite(
		conformance.TestActivityPub, conformance.TestMetadata,
		conformance.TestKey, conformance.TestOAuth, conformance.TestPassword,
	).Run(t, initStorage(t))
}
