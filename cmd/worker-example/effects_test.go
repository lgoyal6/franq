package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestRedeliveryAfterRestartDoesNotRepeatEffect(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state", "effects.json")
	first, err := applyOnce(path, "41", "settle invoice")
	if err != nil || !first {
		t.Fatalf("first application = %t, %v", first, err)
	}
	// Reopening the file models a worker restart after durable effect and before ack.
	second, err := applyOnce(path, "41", "settle invoice")
	if err != nil || second {
		t.Fatalf("redelivery application = %t, %v", second, err)
	}
	var ledger effectLedger
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &ledger); err != nil {
		t.Fatal(err)
	}
	if len(ledger.Effects) != 1 {
		t.Fatalf("effect count = %d, want one", len(ledger.Effects))
	}
}

func TestSameMessageIDWithDifferentBodyFailsClosed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "effects.json")
	if _, err := applyOnce(path, "41", "first body"); err != nil {
		t.Fatal(err)
	}
	if _, err := applyOnce(path, "41", "changed body"); err == nil {
		t.Fatal("expected conflicting replay to fail")
	}
}
