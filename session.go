package mux

import (
	"io"
	"net"
	"reflect"
	"time"

	E "github.com/sagernet/sing/common/exceptions"
	"github.com/sagernet/smux"

	"github.com/hashicorp/yamux"
)

type abstractSession interface {
	Open() (net.Conn, error)
	Accept() (net.Conn, error)
	NumStreams() int
	Close() error
	IsClosed() bool
	CanTakeNewRequest() bool
}

func newClientSession(conn net.Conn, protocol byte) (abstractSession, error) {
	switch protocol {
	case ProtocolH2Mux:
		session, err := newH2MuxClient(conn)
		if err != nil {
			return nil, err
		}
		return session, nil
	case ProtocolSmux:
		client, err := smux.Client(conn, smuxConfig())
		if err != nil {
			return nil, err
		}
		return &smuxSession{client}, nil
	case ProtocolYAMux:
		checkYAMuxConn(conn)
		client, err := yamux.Client(conn, yaMuxConfig())
		if err != nil {
			return nil, err
		}
		return &yamuxSession{client}, nil
	default:
		return nil, E.New("unexpected protocol ", protocol)
	}
}

func newServerSession(conn net.Conn, protocol byte) (abstractSession, error) {
	switch protocol {
	case ProtocolH2Mux:
		return newH2MuxServer(conn), nil
	case ProtocolSmux:
		client, err := smux.Server(conn, smuxConfig())
		if err != nil {
			return nil, err
		}
		return &smuxSession{client}, nil
	case ProtocolYAMux:
		checkYAMuxConn(conn)
		client, err := yamux.Server(conn, yaMuxConfig())
		if err != nil {
			return nil, err
		}
		return &yamuxSession{client}, nil
	default:
		return nil, E.New("unexpected protocol ", protocol)
	}
}

func checkYAMuxConn(conn net.Conn) {
	if conn.LocalAddr() == nil || conn.RemoteAddr() == nil {
		panic("found net.Conn with nil addr: " + reflect.TypeOf(conn).String())
	}
}

var _ abstractSession = (*smuxSession)(nil)

type smuxSession struct {
	*smux.Session
}

func (s *smuxSession) Open() (net.Conn, error) {
	return s.OpenStream()
}

func (s *smuxSession) Accept() (net.Conn, error) {
	return s.AcceptStream()
}

func (s *smuxSession) CanTakeNewRequest() bool {
	return true
}

type yamuxSession struct {
	*yamux.Session
}

func (y *yamuxSession) CanTakeNewRequest() bool {
	return true
}

type yamuxWrapStream struct {
	*yamux.Stream
}

func (w *yamuxWrapStream) Read(p []byte) (n int, err error) {
	n, err = w.Stream.Read(p)
	return n, wrapError(err)
}

func (w *yamuxWrapStream) Write(p []byte) (n int, err error) {
	n, err = w.Stream.Write(p)
	return n, wrapError(err)
}

func (w *yamuxWrapStream) CloseWrite() error {
	// yamux.Stream.Close() implements half-close semantics:
	// sends FIN, state becomes streamLocalClose, can still read
	return w.Stream.Close()
}

func (w *yamuxWrapStream) Upstream() any {
	return w.Stream
}

func smuxConfig() *smux.Config {
	config := smux.DefaultConfig()
	config.KeepAliveDisabled = true
	return config
}

func yaMuxConfig() *yamux.Config {
	config := yamux.DefaultConfig()
	config.LogOutput = io.Discard
	config.StreamCloseTimeout = TCPTimeout
	config.StreamOpenTimeout = TCPTimeout
	// vload: yamux's own defaults (30s between keepalive pings, 10s to wait
	// for a reply before giving up) mean a session that silently goes bad -
	// a mobile network handover, a brief signal drop, a carrier NAT timeout
	// silently dropping the mapping - isn't detected for up to 40s, on top
	// of whatever the OS's own TCP retransmission backoff adds on top of
	// that for a write that never gets acknowledged. Every stream sharing
	// that one connection (mobile-heavy traffic often keeps a handful of
	// long-lived mux connections warm rather than opening new ones per
	// request) hangs for the entire detection window. Confirmed on a real
	// device: multiple unrelated in-flight connections across different
	// domains stalled together for 51-60 seconds, then all completed
	// together - the signature of one shared connection dying and
	// eventually being recovered, not of anything server- or
	// content-specific. Checking more often and giving up on a
	// non-responsive ping sooner cuts worst-case detection from ~40s to
	// ~15s without meaningfully raising the odds of tripping on a merely
	// slow (not dead) mobile link - a healthy connection replies to a ping
	// in milliseconds regardless.
	config.KeepAliveInterval = 10 * time.Second
	config.ConnectionWriteTimeout = 5 * time.Second
	return config
}
