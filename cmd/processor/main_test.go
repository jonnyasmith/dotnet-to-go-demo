package main

import (
	"io"
	"log/slog"
	"syscall"
	"testing"
	"time"
)

// These exercise the composition root itself, including the signal handling
// that no package-level test can reach. t.Chdir keeps demo.db inside the
// test's temporary directory, because databasePath is relative.
//
// Neither test calls t.Parallel: both send real signals to this process and
// both change the working directory.

func TestRunCompletesNormally(t *testing.T) {
	t.Chdir(t.TempDir())

	done := make(chan error, 1)
	go func() { done <- run(discardTestLogger()) }()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run returned %v, want nil", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("run did not return: the pipeline is deadlocked")
	}
}

func TestRunShutsDownOnInterrupt(t *testing.T) {
	t.Chdir(t.TempDir())

	done := make(chan error, 1)
	go func() { done <- run(discardTestLogger()) }()

	// Interrupt part-way through the run. signal.NotifyContext has replaced
	// the default disposition, so this cancels the context instead of killing
	// the test binary.
	time.Sleep(150 * time.Millisecond)
	if err := syscall.Kill(syscall.Getpid(), syscall.SIGINT); err != nil {
		t.Fatalf("sending SIGINT: %v", err)
	}

	select {
	case err := <-done:
		// A cancelled run is a clean shutdown, not a failure: in-flight
		// inserts are acknowledged and the summary query still succeeds.
		if err != nil {
			t.Fatalf("run returned %v after SIGINT, want nil", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("run ignored SIGINT: shutdown is not wired to the context")
	}
}

func discardTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
