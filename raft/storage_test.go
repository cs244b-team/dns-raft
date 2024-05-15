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

	term2, err := s.GetCurrentTerm()
	if err != nil {
		t.Fatal(err)
	}

	if term != term2 {
		t.Fatal("expected term to be equal")
	}

	term = 56
	err = s.SetCurrentTerm(term)
	if err != nil {
		t.Fatal(err)
	}

	term2, err = s.GetCurrentTerm()
	if err != nil {
		t.Fatal(err)
	}

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

	votedFor2, err := s.GetVotedFor()
	if err != nil {
		t.Fatal(err)
	}

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

	votedFor, err := s.GetVotedFor()
	if err != nil {
		t.Fatal(err)
	}

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

	term2, err := s.GetCurrentTerm()
	if err != nil {
		t.Fatal(err)
	}

	if term2 != term {
		t.Fatal("expected term to be equal")
	}

	votedForOpt, err := s.GetVotedFor()
	if err != nil {
		t.Fatal(err)
	}

	if votedForOpt.HasValue() {
		t.Fatal("expected votedFor to be empty")
	}

	votedFor := 5
	err = s.SetVotedFor(votedFor)
	if err != nil {
		t.Fatal(err)
	}

	votedFor2, err := s.GetVotedFor()
	if err != nil {
		t.Fatal(err)
	}

	if !votedFor2.HasValue() {
		t.Fatal("expected votedFor to be set")
	}

	if votedFor2.Value() != votedFor {
		t.Fatal("expected votedFor to be equal")
	}
}
