package raft

import (
	"os"
	"testing"
)

func TestStorageCurrentTerm(t *testing.T) {
	path := "/tmp/raft-node.state"
	s := NewStableStorage(path)
	defer os.Remove(path)

	term := 20000
	err := s.SetCurrentTerm(term)
	if err != nil {
		t.Fatal(err)
	}

	term2 := s.GetCurrentTerm()

	if term != term2 {
		t.Fatal("expected term to be equal")
	}

	term = 56
	err = s.SetCurrentTerm(term)
	if err != nil {
		t.Fatal(err)
	}

	term2 = s.GetCurrentTerm()

	if term != term2 {
		t.Fatal("expected term to be equal")
	}
}

func TestStorageVotedFor(t *testing.T) {
	path := "/tmp/raft-node.state"
	s := NewStableStorage(path)
	defer os.Remove(path)

	votedFor := 5
	err := s.SetVotedFor(votedFor)
	if err != nil {
		t.Fatal(err)
	}

	votedFor2 := s.GetVotedFor()

	if !votedFor2.HasValue() {
		t.Fatal("expected votedFor to be set")
	}

	if votedFor2.Value() != votedFor {
		t.Fatal("expected votedFor to be equal")
	}
}

func TestStorageVotedForEmpty(t *testing.T) {
	path := "/tmp/raft-node.state"
	s := NewStableStorage(path)
	defer os.Remove(path)

	votedFor := s.GetVotedFor()

	if votedFor.HasValue() {
		t.Fatal("expected votedFor to be empty")
	}
}

func TestStorageAll(t *testing.T) {
	path := "/tmp/raft-node.state"
	s := NewStableStorage(path)
	defer os.Remove(path)

	term := 2000
	err := s.SetCurrentTerm(term)
	if err != nil {
		t.Fatal(err)
	}

	term2 := s.GetCurrentTerm()

	if term2 != term {
		t.Fatal("expected term to be equal")
	}

	votedForOpt := s.GetVotedFor()

	if votedForOpt.HasValue() {
		t.Fatal("expected votedFor to be empty")
	}

	votedFor := 5
	err = s.SetVotedFor(votedFor)
	if err != nil {
		t.Fatal(err)
	}

	votedFor2 := s.GetVotedFor()

	if !votedFor2.HasValue() {
		t.Fatal("expected votedFor to be set")
	}

	if votedFor2.Value() != votedFor {
		t.Fatal("expected votedFor to be equal")
	}
}
