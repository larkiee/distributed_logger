package log

import (
	"log"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	data = []byte("Hello World !!!")
)

func TestCreateAppendRead(t *testing.T){
	f, err := os.CreateTemp("", "store_test")
	defer os.Remove(f.Name())
	require.NoError(t, err)

	s, err := newStore(f)
	require.NoError(t, err)

	ws, pos, err := s.Append(data)
	require.NoError(t, err)
	log.Println(ws, pos)
	log.Println(s.buf.Buffered())

	rb, err := s.Read(0)
	require.NoError(t, err)
	log.Println(string(rb), "---")
}