package log

import (
	"io"
	"os"

	"github.com/tysonmote/gommap"
)

var (
	offLen uint64 = 4
	posLen uint64 = 8
	irLen = offLen + posLen
)

type index struct {
	file *os.File
	size uint64
	mmap gommap.MMap
}

func newIndex(f *os.File, c Config) (*index, error) {
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	ind := &index{
		file: f,
	}
	ind.size = uint64(fi.Size())
	if err = os.Truncate(f.Name(), int64(c.Segment.MaxIndexBytes)); err != nil {
		return nil, err
	}

	if ind.mmap, err = gommap.Map(
		f.Fd(), 
		gommap.PROT_READ|gommap.PROT_WRITE,
		gommap.MAP_SHARED,
	); err != nil {
		return nil, err
	}
	return ind, nil
}

func (ind *index) Close() error {
	err := ind.mmap.Sync(gommap.MS_SYNC)
	if err != nil {
		return err
	}

	if err = ind.file.Sync(); err != nil {
		return err
	}

	return ind.file.Close()
}

func (ind *index) Read(index int32) (offset uint32, pos uint64, err error) {
	if ind.size == 0 {
		return 0, 0, io.EOF
	}
	var startInd uint64
	if index == -1 {
		startInd = uint64(ind.size / irLen) - 1
	} else {
		startInd = uint64(index)
	}

	startPos := startInd * irLen

	if startPos + irLen > ind.size {
		return 0, 0, io.EOF
	}

	offset = enc.Uint32(ind.mmap[startPos: startPos + offLen])
	pos = enc.Uint64(ind.mmap[startPos + offLen: startPos + irLen])

	return offset, pos, nil
}

func (ind *index) Write(offset uint32, pos uint64) error {
	if uint64(len(ind.mmap)) < ind.size + irLen {
		return io.EOF
	} 

	enc.PutUint32(ind.mmap[ind.size: ind.size+offLen], offset)
	enc.PutUint64(ind.mmap[ind.size+offLen: ind.size+irLen], pos)
	
	ind.size += irLen

	return nil
}

func (ind *index) Name() string {
	return ind.file.Name()
}