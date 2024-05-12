package log

import (
	"fmt"
	"io"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/larkiee/distributed_logger/api/v1"
)

type Log struct {
	mu sync.RWMutex

	Dir string
	Config Config

	segments []*segment
	activeSegment *segment
}


func NewLog(dir string, c Config) (*Log, error) {
	l := &Log{
		Dir: dir,
		Config: c,
	}

	if c.Segment.MaxIndexBytes == 0 {
		l.Config.Segment.MaxIndexBytes = 1024
	}

	if c.Segment.MaxStoreBytes == 0 {
		l.Config.Segment.MaxStoreBytes = 1024
	}

	return l, l.setup()
}

func (l *Log) newSegment(bOff uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	ns, err := newSegment(l.Dir, bOff, l.Config)
	if err != nil {
		return err
	}
	l.segments = append(l.segments, ns)
	l.activeSegment = ns
	return nil
}

func (l *Log) setup() error {
	files, err := os.ReadDir(l.Dir)
	if err != nil {
		return err
	}

	var baseOffsets []uint64

	for _, f := range files {
		bOffRaw := strings.TrimSuffix(
			f.Name(),
			path.Ext(f.Name()),
		)
		bOff, err := strconv.ParseUint(bOffRaw, 10, 64)
		if err != nil {
			return err
		}
		baseOffsets = append(baseOffsets, bOff)
	}
	sort.Slice(baseOffsets, func(i, j int) bool {
		return baseOffsets[i] < baseOffsets[j]
	})

	for i := uint64(0); i < uint64(len(baseOffsets)); i += 2 {
		if err = l.newSegment(baseOffsets[i]); err != nil {
			return err
		}
	}

	if l.segments == nil {
		if err = l.newSegment(l.Config.Segment.InitialOffset); err != nil {
			return err
		}
	}

	return nil
}

func (l *Log) Append(r *api.Record) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	off, err := l.activeSegment.Append(r)
	if err != nil {
		return 0, err
	}
	if l.activeSegment.IsMaxed() {
		err = l.newSegment(off + 1)
	}
	return off, err
}

func (l *Log) Read(off uint64) (*api.Record, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	var s *segment
	for _, seg := range l.segments {
		if off >= seg.baseOffset && off <= seg.nextOffset {
			s = seg
		}
	}
	if s == nil || off >= s.nextOffset {
		return nil, fmt.Errorf("offset %d out of range", off)
	}
	return s.Read(off)
}

func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	var err error
	for _, seg := range l.segments {
		if err = seg.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (l *Log) Remove() error {
	if err := l.Close(); err != nil {
		return err
	}

	return os.RemoveAll(l.Dir)
}

func (l *Log) Reset() error {
	if err := l.Remove(); err != nil {
		return err
	}

	return l.setup()
}

func (l *Log) LowestOffset() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.segments[0].baseOffset
}

func (l *Log) HighestOffset() uint64 {
	return l.activeSegment.nextOffset - 1
}

func (l *Log) Truncate(off uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	var segments []*segment
	var err error
	for _, seg := range l.segments {
		if seg.nextOffset <= off + 1 {
			err = seg.Remove()
			if err != nil {
				return err
			}
		}else {
			segments = append(segments, seg)
		}
	}
	l.segments = segments
	l.activeSegment = l.segments[len(segments) - 1]
	return nil
}

type originReader struct {
	*store
	offset uint64
}

func (or *originReader) Read(d []byte) (int, error) {
	n, err := or.ReadAt(d, or.offset)
	if err != nil {
		return 0, err
	}
	or.offset += uint64(n)
	return n, nil
}

func (l *Log) Reader() io.Reader {
	l.mu.RLock()
	defer l.mu.RUnlock()

	readers := make([]io.Reader, len(l.segments))
	for i, seg := range l.segments {
		readers[i] = &originReader{seg.store, 0}
	}
	return io.MultiReader(readers...)
}
