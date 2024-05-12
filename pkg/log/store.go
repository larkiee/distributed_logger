package log

import (
	"bufio"
	"encoding/binary"
	"log"
	"os"
	"sync"
)

var enc = binary.BigEndian

const (
	sepLen uint64 = 8
)

type store struct {
	*os.File
	mu sync.RWMutex
	buf *bufio.Writer
	size uint64
}

func newStore(f *os.File) (*store, error) {
	log.Println("NAME: ", f.Name())
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	size := uint64(fi.Size())
	log.Println("SIZE: ", size)
	return &store{
		File: f,
		size: size,
		buf: bufio.NewWriter(f),
	}, nil
}

func (s *store) Append(d []byte) (uint64, uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	pos := s.size
	if err := binary.Write(s.buf, enc, uint64(len(d))); err != nil {
		return 0, 0, err
	}
	wn, err := s.buf.Write(d)
	if err != nil {
		return 0, 0, err
	}
	w := uint64(wn) + sepLen
	s.size += w
	return w, pos, nil
}

func (s *store) Read(pos uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.buf.Flush(); err != nil {
		return nil, err
	}

	ib := make([]byte, sepLen)
	if _, err := s.File.ReadAt(ib, int64(pos)); err != nil {
		return nil, err
	}

	b := make([]byte, enc.Uint64(ib))

	if _, err := s.File.ReadAt(b, int64(pos + sepLen)); err != nil {
		return nil, err
	}

	return b, nil
}

func (s *store) ReadAt(b []byte, off uint64) (nn int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.buf.Flush(); err != nil {
		return 0, err
	}

	return s.File.ReadAt(b,  int64(off))
}
