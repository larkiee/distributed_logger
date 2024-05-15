package discovery

import (
	"fmt"
	"testing"
	"time"

	"github.com/hashicorp/serf/serf"
	"github.com/stretchr/testify/require"
	"github.com/travisjeffery/go-dynaport"
)


func TestDoscovery(t *testing.T){
	m := make([]*Membership, 0)
	m, h := setupMember(t, m)
	m, _ = setupMember(t, m)
	m, _ = setupMember(t, m)

	require.Eventually(t, func() bool {
		return len(m) == 3 &&
				len(h.joins) == 2 &&
				len(h.leaves) == 0	
	}, 3 * time.Second, 250 * time.Millisecond)

	m[2].Leave()

	require.Eventually(t, func() bool {
		return len(h.leaves) == 1 &&
				serf.StatusLeft == m[0].Members()[2].Status	&&
				len(m[0].Members()) == 3
	}, 3 * time.Second, 250 * time.Millisecond)

	require.Equal(t, fmt.Sprintf("%d", 2), <- h.leaves)
}


func setupMember(t *testing.T, members []*Membership) ([]*Membership, *handler){
	t.Helper()
	id := fmt.Sprintf("%d", len(members))
	port := dynaport.Get(1)
	addr := fmt.Sprintf("%s:%d", "127.0.0.1", port[0])
	tags := map[string]string{
		"id": id,
		"addr": addr,
	}
	c := Config{
		NodeName: id,
		BindAddr: addr,
		Tags: tags,
	}

	h := &handler{}
	
	if len(members) == 0 {
		h.joins = make(chan map[string]string, 3)
		h.leaves = make(chan string, 3)
	} else {
		c.StartJoinAddrs = []string{members[0].BindAddr}
	}
	m, _ := NewMembership(c, h)
	members = append(members, m)
	return members, h
}

type handler struct{
	joins chan map[string]string
	leaves chan string
}

func (h *handler) Join(id, addr string) error {
	h.joins <- map[string]string{
		"id": id,
		"addr": addr,
	}
	return nil
}

func (h *handler) Leave(id string) error {
	h.leaves <- id
	return nil
}