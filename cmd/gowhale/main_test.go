package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestRunDispatchesDoctor(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run([]string{"doctor", "-h"}, &stdout, &stderr); code != 0 {
		t.Fatalf("doctor -h = %d, want 0: %s", code, stderr.String())
	}
	if !strings.Contains(stderr.String(), "Usage of doctor") {
		t.Fatalf("doctor help was not printed: %q", stderr.String())
	}
}

func TestRunDispatchesVersion(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run([]string{"version"}, &stdout, &stderr); code != 0 {
		t.Fatalf("version = %d, want 0: %s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "gowhale ") {
		t.Fatalf("version output does not name the product: %q", stdout.String())
	}
}

func TestRunRejectsUnknownCommand(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run([]string{"migrate-everything"}, &stdout, &stderr); code != 2 {
		t.Fatalf("unknown command = %d, want 2", code)
	}
	if !strings.Contains(stderr.String(), "unknown command") {
		t.Fatalf("unknown command was not named: %q", stderr.String())
	}
}

func TestRunServeRejectsInvalidConfiguration(t *testing.T) {
	t.Setenv("GOWHALE_CONFIG_FILE", "")
	t.Setenv("GOWHALE_QUESTDB_ENABLED", "true")
	t.Setenv("GOWHALE_QUESTDB_PG_PASSWORD", "")
	var stdout, stderr bytes.Buffer
	if code := run([]string{"serve"}, &stdout, &stderr); code != 1 {
		t.Fatalf("serve with invalid config = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "QuestDB PG password") {
		t.Fatalf("configuration refusal was not named: %q", stderr.String())
	}
}
