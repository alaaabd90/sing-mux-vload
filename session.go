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
	// vload: only KeepAliveInterval is touched here - ConnectionWriteTimeout
	// is left at yamux's own default (10s) deliberately. yamux reuses that
	// single field for two very different purposes: how long to wait for a
	// keepalive ping's reply, and how long to wait for any regular stream
	// write to succeed. An earlier attempt lowered ConnectionWriteTimeout
	// to 5s to speed up the first case, and that config was live-tested:
	// worst-case dead-session detection did drop, but real (not dead),
	// merely congested writes under normal mobile-network conditions -
	// which legitimately need more than 5s to flush sometimes - started
	// getting killed early too, at exactly the new 5s ceiling. Confirmed on
	// a real device: thousands of connections clustering tightly around
	// 5.8-5.9s where they previously would have completed normally. That
	// change was fully reverted.
	//
	// Only shortening the interval between keepalive pings - not the reply
	// timeout - avoids that tradeoff entirely: a healthy connection still
	// gets the same generous 10s before a write or a ping reply counts as
	// failed, exactly as before. The only change is checking every 12s
	// instead of every 30s while otherwise idle, so a session that's
	// actually gone dead (no traffic to notice it via a failed write) is
	// still discovered in ~12s+10s=~22s worst case instead of ~30s+10s=40s,
	// with no change to how long a genuinely slow-but-alive write is given
	// to complete.
	config.KeepAliveInterval = 12 * time.Second
	return config
}
