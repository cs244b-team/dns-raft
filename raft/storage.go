package raft

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"sync"

	log "github.com/sirupsen/logrus"
)

type Storage interface {
	GetCurrentTerm() (int, error)
	SetCurrentTerm(term int) error
	GetVotedFor() (Optional[int], error)
	SetVotedFor(votedFor int) error
}

// StableStorage is a two-line file that stores the current term and the voted-for node ID
type StableStorage struct {
	rwLock sync.RWMutex
	path   string
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

	return &StableStorage{
		rwLock: sync.RWMutex{},
		path:   path,
	}
}

func (s *StableStorage) Reset() error {
	s.rwLock.Lock()
	defer s.rwLock.Unlock()
	return s.write(0, -1)
}

func (s *StableStorage) GetCurrentTerm() (int, error) {
	s.rwLock.RLock()
	defer s.rwLock.RUnlock()
	return s.getCurrentTerm()
}

func (s *StableStorage) GetVotedFor() (Optional[int], error) {
	s.rwLock.RLock()
	defer s.rwLock.RUnlock()
	return s.getVotedFor()
}

func (s *StableStorage) SetCurrentTerm(term int) error {
	s.rwLock.Lock()
	defer s.rwLock.Unlock()

	votedFor, err := s.getVotedFor()
	if err != nil {
		return err
	}

	return s.write(term, votedFor.ValueOr(-1))
}

func (s *StableStorage) SetVotedFor(votedFor int) error {
	s.rwLock.Lock()
	defer s.rwLock.Unlock()

	currentTerm, err := s.getCurrentTerm()
	if err != nil {
		return err
	}

	return s.write(currentTerm, votedFor)
}

func (s *StableStorage) getCurrentTerm() (int, error) {
	file, err := os.Open(s.path)
	if err != nil {
		return 0, err
	}

	scanner := bufio.NewScanner(file)
	scanner.Scan()

	currentTerm, err := strconv.Atoi(scanner.Text())
	if err != nil {
		return 0, err
	}

	return currentTerm, nil
}

func (s *StableStorage) getVotedFor() (Optional[int], error) {
	file, err := os.Open(s.path)
	if err != nil {
		return None[int](), err
	}

	scanner := bufio.NewScanner(file)
	scanner.Scan()
	scanner.Scan()

	votedFor, err := strconv.Atoi(scanner.Text())
	if err != nil {
		return None[int](), err
	}

	if votedFor == -1 {
		return None[int](), nil
	}

	return Some(votedFor), nil
}

// Create a temporary file to write the new state, then rename it to the original file
func (s *StableStorage) write(currentTerm int, votedFor int) error {
	file, err := os.OpenFile(s.path+".tmp", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}

	str := fmt.Sprintf("%d\n%d\n", currentTerm, votedFor)
	_, err = file.WriteString(str)
	if err != nil {
		return err
	}

	err = os.Rename(s.path+".tmp", s.path)
	if err != nil {
		return err
	}

	return nil
}
