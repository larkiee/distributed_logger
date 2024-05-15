package discovery

import (
	"net"

	"github.com/hashicorp/serf/serf"
	"go.uber.org/zap"
)


type Membership struct {
	Config
	handler Handler
	serf *serf.Serf
	events chan serf.Event
	logger *zap.Logger
}

type Config struct {
	NodeName string
	BindAddr string
	Tags map[string]string
	StartJoinAddrs []string
}

type Handler interface {
	Join(name, addr string) error
	Leave(name string) error 
}

func NewMembership(c Config, h Handler) (*Membership, error){
	m := &Membership{
		Config: c,
		handler: h,
		logger: zap.L().Named("membership"),
	}
	
	if err := m.initSerif(); err != nil {
		return nil, err
	}
	return m, nil
}

func (m *Membership) initSerif() error{
	addr, err := net.ResolveTCPAddr("tcp", m.BindAddr)
	if err != nil {
		return err
	}

	sc := serf.DefaultConfig()
	sc.Init()
	sc.MemberlistConfig.BindAddr = addr.IP.String()
	sc.MemberlistConfig.BindPort = addr.Port
	sc.Tags = m.Tags
	sc.NodeName = m.NodeName

	eChan := make(chan serf.Event)
	m.events = eChan
	sc.EventCh = eChan

	m.serf, err = serf.Create(sc)
	if err != nil {
		return err
	}
	go m.handleEvents()
	if m.StartJoinAddrs != nil {
		_, err := m.serf.Join(m.StartJoinAddrs, true)
		if err != nil {
			return nil
		}
	}
	return nil
}

func (m *Membership) handleEvents() {
	for e := range m.events {
		switch e.EventType(){
		case serf.EventMemberJoin:
			e := e.(serf.MemberEvent)
			for _, member := range e.Members {
				if m.isLocal(member) {
					continue
				}
				m.handleJoin(member)
			}
		case serf.EventMemberLeave, serf.EventMemberFailed:
			e := e.(serf.MemberEvent)
			for _, member := range e.Members {
				if m.isLocal(member){
					continue
				}
				m.handleLeave(member)
			}
		} 
	}
}

func (m *Membership) isLocal(member serf.Member) bool {
	return m.serf.LocalMember().Name == member.Name
}

func (m *Membership) handleJoin(member serf.Member) {
	if err := m.handler.Join(member.Name, member.Tags["rpc_addr"]); err != nil {
		m.errLog(err, "error in join", member)
	}
}

func (m *Membership) handleLeave(member serf.Member){
	if err := m.handler.Leave(member.Name); err != nil {
		m.errLog(err, "error in leave cluster", member)
	}
}

func (m *Membership) errLog(err error, msg string, member serf.Member){
	m.logger.Error(
		msg,
		zap.Error(err),
		zap.String("name", member.Name),
		zap.String("rpc_addr", member.Tags["rpc_addr"]),
	)
}

func (m *Membership) Members() []serf.Member {
	return m.serf.Members()
}

func (m *Membership) Leave() error {
	return m.serf.Leave()
}