package db

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"time"

	"cloud.google.com/go/cloudsqlconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

var Pool *pgxpool.Pool

// Connect initializes the global pgx Pool using the Cloud SQL Go connector.
// Required envs: DB_USER, DB_PASSWORD, DB_NAME, INSTANCE_CONNECTION_NAME
// Optional: PRIVATE_IP (any non-empty value enables Private IP)
func Connect() error {
	dbUser := os.Getenv("DB_USER")
	dbPass := os.Getenv("DB_PASSWORD")
	dbName := os.Getenv("DB_NAME")
	instance := os.Getenv("INSTANCE_CONNECTION_NAME")
	usePrivate := os.Getenv("PRIVATE_IP") != ""

	if dbUser == "" || dbPass == "" || dbName == "" || instance == "" {
		return fmt.Errorf("missing DB envs (DB_USER/DB_PASSWORD/DB_NAME/INSTANCE_CONNECTION_NAME)")
	}

	// Note: pgx uses `dbname` or `database`; both are accepted by pgxpool.ParseConfig.
	dsn := fmt.Sprintf("user=%s password=%s database=%s sslmode=disable", dbUser, dbPass, dbName)

	opts := []cloudsqlconn.Option{}
	if usePrivate {
		opts = append(opts, cloudsqlconn.WithDefaultDialOptions(cloudsqlconn.WithPrivateIP()))
	}
	dialer, err := cloudsqlconn.NewDialer(context.Background(), opts...)
	if err != nil {
		return fmt.Errorf("cloudsql dialer: %w", err)
	}

	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return fmt.Errorf("pgxpool.ParseConfig: %w", err)
	}

	// Replace net dialer with Cloud SQL connector dialer
	cfg.ConnConfig.DialFunc = func(ctx context.Context, network, addr string) (net.Conn, error) {
		return dialer.Dial(ctx, instance)
	}

	// Pool sizing — tune as you like
	cfg.MinConns = 0
	cfg.MaxConns = 10
	cfg.MaxConnIdleTime = 5 * time.Minute

	Pool, err = pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		return fmt.Errorf("pgxpool.NewWithConfig: %w", err)
	}
	if err := Pool.Ping(context.Background()); err != nil {
		return fmt.Errorf("db ping: %w", err)
	}
	log.Println("CONNECTED TO DATABASE")
	return nil
}
