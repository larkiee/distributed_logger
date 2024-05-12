package log

import (
	"log"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)



func TestIndexCreate(t *testing.T){
	f, err := os.CreateTemp("", "index_test")
	defer os.Remove(f.Name())
	require.NoError(t, err)
	cfg := Config{
		Segment: SegmentConfig{
			MaxIndexBytes: 2048,
		},
	}
	ind, err := newIndex(f, cfg)
	require.NoError(t, err)
	
	for o, p := uint32(0), uint64(0); o < 10 && p < 1000; o, p = o + 1, p + 100 {
		err = ind.Write(o, p)
		require.NoError(t, err)
	}

	off, pos, err := ind.Read(0)
	require.NoError(t, err)
	log.Println(off, pos)
	
	off, pos, err = ind.Read(5)
	require.NoError(t, err)
	log.Println(off, pos)
}