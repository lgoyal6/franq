// Package consumer contains the durable effect boundary used by the example
// Franq worker. PostgreSQL commits the effect row and message-id dedup key in
// one transaction, so redelivery after commit but before queue ack is a no-op.
package consumer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresEffects struct {
	pool *pgxpool.Pool
}

func ConnectPostgres(ctx context.Context, url string) (*PostgresEffects, error) {
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("create PostgreSQL pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("connect to PostgreSQL: %w", err)
	}
	_, err = pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS franq_consumer_effects (
		message_id TEXT PRIMARY KEY,
		body_sha256 CHAR(64) NOT NULL,
		body TEXT NOT NULL,
		committed_at TIMESTAMPTZ NOT NULL DEFAULT now()
	)`)
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("create consumer effect table: %w", err)
	}
	return &PostgresEffects{pool: pool}, nil
}

func (p *PostgresEffects) Close() { p.pool.Close() }

// ApplyOnce records a business effect represented by the effect row itself.
// The primary key and row commit atomically; this does not claim atomicity with
// an unrelated external side effect.
func (p *PostgresEffects) ApplyOnce(ctx context.Context, messageID, body string) (bool, error) {
	if messageID == "" {
		return false, errors.New("message id is required")
	}
	digest := sha256.Sum256([]byte(body))
	hash := hex.EncodeToString(digest[:])
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin effect transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	tag, err := tx.Exec(ctx, `INSERT INTO franq_consumer_effects(message_id, body_sha256, body)
		VALUES ($1, $2, $3) ON CONFLICT (message_id) DO NOTHING`, messageID, hash, body)
	if err != nil {
		return false, fmt.Errorf("write consumer effect: %w", err)
	}
	if tag.RowsAffected() == 0 {
		var previous string
		if err := tx.QueryRow(ctx, `SELECT body_sha256 FROM franq_consumer_effects WHERE message_id = $1`, messageID).Scan(&previous); err != nil {
			return false, fmt.Errorf("read prior consumer effect: %w", err)
		}
		if previous != hash {
			return false, fmt.Errorf("message %s was replayed with a different body", messageID)
		}
		if err := tx.Commit(ctx); err != nil {
			return false, fmt.Errorf("commit duplicate check: %w", err)
		}
		return false, nil
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit consumer effect: %w", err)
	}
	return true, nil
}
