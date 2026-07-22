package fsm

import (
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/oklog/ulid/v2"
)

func TestErrorHelpers(t *testing.T) {
	if Abort(nil) != nil {
		t.Fatal("Abort(nil) should be nil")
	}
	if halt(nil) != nil {
		t.Fatal("halt(nil) should be nil")
	}
	if Handoff(ulid.ULID{}) != nil {
		t.Fatal("Handoff of a zero version should be nil")
	}

	wrapped := Abort(io.EOF)
	if _, ok := errors.AsType[*AbortError](wrapped); !ok {
		t.Fatalf("expected AbortError, got %T", wrapped)
	}
	if !errors.Is(wrapped, io.EOF) {
		t.Fatal("AbortError should unwrap to the original error")
	}

	halted := halt(io.EOF)
	if _, ok := errors.AsType[*haltError](halted); !ok {
		t.Fatalf("expected haltError, got %T", halted)
	}
	if !errors.Is(halted, io.EOF) {
		t.Fatal("haltError should unwrap to the original error")
	}

	version := ulid.Make()
	handoff := Handoff(version)
	hoe, ok := errors.AsType[*HandoffError](handoff)
	if !ok {
		t.Fatalf("expected HandoffError, got %T", handoff)
	}
	if hoe.NewFSM != version {
		t.Fatalf("expected handoff version %s, got %s", version, hoe.NewFSM)
	}
	if !strings.Contains(handoff.Error(), version.String()) {
		t.Fatalf("expected handoff message to include the version, got %q", handoff.Error())
	}

	sysErr := NewUnrecoverableSystemError(io.EOF)
	if sysErr.Kind != ErrorKindSystem {
		t.Fatalf("expected system kind, got %s", sysErr.Kind)
	}
	if !errors.Is(sysErr, io.EOF) {
		t.Fatal("UnrecoverableError should unwrap to the original error")
	}
	if NewUnrecoverableUserError(io.EOF).Kind != ErrorKindUser {
		t.Fatal("expected user kind")
	}
}
