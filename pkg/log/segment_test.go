package log

import (
	"log"
	"os"
	"testing"

	"github.com/larkiee/distributed_logger/api/v1"
	"github.com/stretchr/testify/require"
)


func TestSegment(t *testing.T) {
	dir := os.TempDir()
	defer func ()  {
		err := os.RemoveAll(dir)
		if err != nil {
			log.Println("temp directory did not remove")
		}	
	}()

	c := Config{}
	c.Segment.MaxIndexBytes = 5 * irLen
	c.Segment.MaxStoreBytes = 1024

	seg, err := newSegment(dir, 10, c)
	require.NoError(t, err)

	require.False(t, seg.IsMaxed())
	require.Equal(t, uint64(10), seg.baseOffset, seg.nextOffset)

	tr := &api.Record{
		Value: []byte("Hello World !!!"),
	}
	for i := uint64(0); i < 5; i++ {
		off, err := seg.Append(tr)
		require.NoError(t, err)

		rr, err := seg.Read(off)
		require.NoError(t, err)
		require.Equal(t, seg.baseOffset + i, rr.Offset)
		require.Equal(t, tr.Value, rr.Value)
	}

	require.True(t, seg.IsMaxed())

}