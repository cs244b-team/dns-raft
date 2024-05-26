package raft

import (
	"fmt"
	"os"

	log "github.com/sirupsen/logrus"
)

const (
	StorageFmtStr = "%d\n%d\n"
)

// StableStorage is a two-line file that stores the current term and the voted-for node ID
type StableStorage struct {
	file        *os.File
	currentTerm int
	votedFor    Optional[int]
}

func NewStableStorage(path string) *StableStorage {
	// Check if the file exists, otherwise create it
	if _, err := os.Stat(path); os.IsNotExist(err) {
		bytes := []byte("0\n-1\n")
		err := os.WriteFile(path, bytes, 0644)
		if err != nil {
			log.Fatalf("failed to create storage file: %s", err)
		}
	}

	// Open the file for reading and writing
	file, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		log.Fatalf("failed to open storage file: %s", err)
	}

	log.Debugf("opened storage file: %s", path)
	ss := StableStorage{file, 0, None[int]()}
	_term, _votedFor, err := ss.read()
	if err == nil {
		ss.currentTerm = _term
		ss.votedFor = _votedFor
	} else {
		log.Fatalf("failed to re-read storage file: %s", err)
	}
	return &ss
}

func (s *StableStorage) Close() error {
	return s.file.Close()
}

func (s *StableStorage) Reset() error {
	return s.write(0, -1)
}

func (s *StableStorage) GetCurrentTerm() int {
	return s.currentTerm
}

func (s *StableStorage) GetVotedFor() Optional[int] {
	return s.votedFor
}

func (s *StableStorage) SetCurrentTerm(term int) error {
	return s.write(term, s.GetVotedFor().ValueOr(-1))
}

func (s *StableStorage) SetVotedFor(votedFor int) error {
	return s.write(s.GetCurrentTerm(), votedFor)
}

func (s *StableStorage) read() (int, Optional[int], error) {
	_, err := s.file.Seek(0, 0)
	if err != nil {
		return 0, None[int](), err
	}

	var currentTerm int
	var votedFor int
	_, err = fmt.Fscanf(s.file, StorageFmtStr, &currentTerm, &votedFor)
	if err != nil {
		return 0, None[int](), err
	}

	if votedFor == -1 {
		return currentTerm, None[int](), nil
	}

	return currentTerm, Some(votedFor), nil
}

func (s *StableStorage) write(currentTerm int, votedFor int) error {
	_, err := s.file.Seek(0, 0)
	if err != nil {
		return err
	}

	str := fmt.Sprintf(StorageFmtStr, currentTerm, votedFor)
	_, err = s.file.WriteString(str)
	if err != nil {
		return err
	}

	return nil
}
