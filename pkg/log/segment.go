package log

import (
	"fmt"
	"os"
	"path"

	"github.com/larkiee/distributed_logger/api/v1"
	"google.golang.org/protobuf/proto"
)

type segment struct {
	store *store
	index *index
	baseOffset, nextOffset uint64
	config Config
}

func newSegment(dir string, bOff uint64, c Config) (*segment, error) {
	seg := &segment{
		baseOffset: bOff,
		config: c,
	}

	f, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d.%s", bOff, "store")),
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644,
	)

	if err != nil {
		return nil, err
	}

	if seg.store, err = newStore(f); err != nil {
		return nil, err
	}

	f, err = os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d.%s", bOff, "index")),
		os.O_RDWR|os.O_CREATE,
		0644,
	)

	if err != nil {
		return nil, err
	}

	if seg.index, err = newIndex(f, c); err != nil {
		return nil, err
	}

	if off, _, err := seg.index.Read(-1); err != nil {
		seg.nextOffset = bOff
	} else {
		seg.nextOffset = bOff + uint64(off) + 1
	}

	return seg, nil
}

func (seg *segment) Append(r *api.Record) (offset uint64, err error){
	cur := seg.nextOffset
	r.Offset = cur

	b, err := proto.Marshal(r)
	if err != nil {
		return 0, nil
	}

	_, pos, err := seg.store.Append(b)
	if err != nil {
		return 0, err
	}

	err = seg.index.Write(
		uint32(seg.nextOffset - seg.baseOffset),
		pos,
	)

	if err != nil {
		return 0, err
	}

	seg.nextOffset++;

	return cur, nil
}

func (seg *segment) Read(offset uint64) (*api.Record, error) {
	_, pos, err := seg.index.Read(int32(offset - seg.baseOffset))
	if err != nil {
		return nil, err
	}

	b, err := seg.store.Read(pos)
	if err != nil {
		return nil, err
	}

	var r *api.Record = &api.Record{}
	err = proto.Unmarshal(b, r)
	if err != nil {
		return nil, err
	}

	return r, nil
}

func (seg *segment) IsMaxed() bool {
	return seg.store.size >= seg.config.Segment.MaxStoreBytes || 
			seg.index.size >= seg.config.Segment.MaxIndexBytes
}

func (seg *segment) Close() error {
	err := seg.store.Close()
	if err != nil {
		return err
	}

	if err = seg.index.Close(); err != nil {
		return err
	}

	return nil
}

func (seg *segment) Remove() error {
	err := seg.Close()
	if err != nil {
		return err
	}

	if err = os.Remove(seg.store.Name()); err != nil {
		return err
	}

	if err = os.Remove(seg.index.Name()); err != nil {
		return err
	}

	return nil
}

// func nearestMultiple(j, k uint64) uint64 {
// 	if j >= 0 {
// 		return (j / k) * k
// 	}
// 	return ((j - k + 1) / k) * k
// }