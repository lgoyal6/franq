package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

type effectRecord struct {
	BodySHA256 string `json:"body_sha256"`
}

type effectLedger struct {
	SchemaVersion int                     `json:"schema_version"`
	Effects       map[string]effectRecord `json:"effects"`
}

// applyOnce persists the example consumer's effect and deduplication key in
// one atomic file replacement. A replay after a crash is a no-op. This sample
// demonstrates local file effects, not atomicity with an external database.
func applyOnce(path, messageID, body string) (bool, error) {
	if messageID == "" {
		return false, errors.New("message id is required")
	}
	var ledger effectLedger
	if raw, err := os.ReadFile(path); err == nil {
		if err := json.Unmarshal(raw, &ledger); err != nil {
			return false, fmt.Errorf("read effect ledger: %w", err)
		}
		if ledger.SchemaVersion != 1 || ledger.Effects == nil {
			return false, errors.New("unsupported effect ledger")
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return false, err
	} else {
		ledger = effectLedger{SchemaVersion: 1, Effects: make(map[string]effectRecord)}
	}

	digest := sha256.Sum256([]byte(body))
	value := effectRecord{BodySHA256: hex.EncodeToString(digest[:])}
	if previous, ok := ledger.Effects[messageID]; ok {
		if previous != value {
			return false, fmt.Errorf("message %s was replayed with a different body", messageID)
		}
		return false, nil
	}
	ledger.Effects[messageID] = value
	encoded, err := json.MarshalIndent(ledger, "", "  ")
	if err != nil {
		return false, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return false, err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".effects-*.tmp")
	if err != nil {
		return false, err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.Write(append(encoded, '\n')); err != nil {
		tmp.Close()
		return false, err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return false, err
	}
	if err := tmp.Close(); err != nil {
		return false, err
	}
	if err := os.Rename(tmpName, path); err != nil {
		return false, err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return false, err
	}
	defer dir.Close()
	if err := dir.Sync(); err != nil {
		return false, err
	}
	return true, nil
}
