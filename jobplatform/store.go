package jobplatform

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type Store struct {
	path string
	mu   sync.Mutex
	data *storeData
}

type storeData struct {
	Jobs     map[string]*Job `json:"jobs"`
	Sequence int             `json:"sequence"`
}

func OpenStore(path string) (*Store, error) {
	s := &Store{path: path, data: &storeData{Jobs: make(map[string]*Job)}}
	content, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return s, s.persistLocked()
	}
	if err != nil {
		return nil, err
	}
	if len(content) > 0 {
		if err := json.Unmarshal(content, s.data); err != nil {
			return nil, fmt.Errorf("load job store: %w", err)
		}
	}
	if s.data.Jobs == nil {
		s.data.Jobs = make(map[string]*Job)
	}
	return s, nil
}

func (s *Store) Update(mutate func(*storeData) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := mutate(s.data); err != nil {
		return err
	}
	return s.persistLocked()
}

func (s *Store) View(fn func(*storeData) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return fn(s.data)
}

func (s *Store) Snapshot() (map[string]Job, int, error) {
	out := make(map[string]Job)
	err := s.View(func(data *storeData) error {
		for id, job := range data.Jobs {
			out[id] = *job
		}
		return nil
	})
	return out, 0, err
}

func (s *Store) persistLocked() error {
	content, err := json.MarshalIndent(s.data, "", "  ")
	if err != nil {
		return err
	}
	dir := filepath.Dir(s.path)
	tmp, err := os.CreateTemp(dir, ".jobstore-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.Write(content); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, s.path)
}
