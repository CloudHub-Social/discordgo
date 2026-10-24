package discordgo

import (
	"bytes"
	"compress/zlib"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

type handshakeTestFrame struct {
	Op int `json:"op"`
}

// Each fixture owns its hijacked WebSocket and can release a blocked Open
// without taking Session's lock. No Discord credentials or network are used.
func newHandshakeTestGateway(t *testing.T, exchange func(*websocket.Conn) error) (string, func()) {
	t.Helper()
	accepted := make(chan *websocket.Conn, 1)
	release := make(chan struct{})
	done := make(chan struct{})
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(done)
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade test gateway: %v", err)
			return
		}
		accepted <- conn
		defer conn.Close()
		if err := exchange(conn); err != nil {
			t.Errorf("test gateway exchange: %v", err)
			return
		}
		<-release
	}))
	var once sync.Once
	stop := func() {
		once.Do(func() {
			close(release)
			select {
			case conn := <-accepted:
				_ = conn.Close()
				select {
				case <-done:
				case <-time.After(2 * time.Second):
					t.Error("test gateway did not stop")
				}
			default:
			}
			server.Close()
		})
	}
	t.Cleanup(stop)
	return "ws" + strings.TrimPrefix(server.URL, "http"), stop
}

func newHandshakeTestSession(t *testing.T, gateway string) *Session {
	t.Helper()
	session, err := New("Bot handshake-test")
	if err != nil {
		t.Fatal(err)
	}
	session.EventHandler = func(any) {}
	session.ShouldReconnectOnError = false
	dialer := *websocket.DefaultDialer
	dialer.Proxy = nil
	dialer.HandshakeTimeout = time.Second
	session.Dialer = &dialer
	session.SetGatewayURL(gateway)
	session.GatewayHandshakeTimeout = 250 * time.Millisecond
	return session
}

func handshakeTestHello(conn *websocket.Conn) error {
	if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		return err
	}
	if err := conn.WriteJSON(map[string]any{
		"op": 10, "d": map[string]any{"heartbeat_interval": 3_600_000},
	}); err != nil {
		return err
	}
	var identify handshakeTestFrame
	if err := conn.ReadJSON(&identify); err != nil {
		return err
	}
	if identify.Op != 2 {
		return fmt.Errorf("IDENTIFY opcode = %d, want 2", identify.Op)
	}
	return nil
}

func handshakeTestReady(conn *websocket.Conn) error {
	if err := handshakeTestHello(conn); err != nil {
		return err
	}
	return conn.WriteJSON(map[string]any{
		"op": 0, "t": "READY", "s": 42,
		"d": map[string]any{
			"v": 9, "session_id": "handshake-test",
			"user":   map[string]any{"id": "1", "username": "handshake-test", "bot": true},
			"guilds": []any{}, "private_channels": []any{},
		},
	})
}

func TestGatewayHandshakeTimeout(t *testing.T) {
	for _, tc := range []struct {
		name     string
		exchange func(*websocket.Conn) error
	}{
		{name: "missing HELLO", exchange: func(*websocket.Conn) error { return nil }},
		{name: "missing READY", exchange: handshakeTestHello},
		{name: "partial compressed HELLO", exchange: func(conn *websocket.Conn) error {
			return conn.WriteMessage(websocket.BinaryMessage, []byte{0x78})
		}},
		{name: "partial compressed READY", exchange: func(conn *websocket.Conn) error {
			if err := handshakeTestHello(conn); err != nil {
				return err
			}
			var compressed bytes.Buffer
			writer := zlib.NewWriter(&compressed)
			defer writer.Close()
			if _, err := writer.Write([]byte(`{"op":0,"t":"READY","s":42,"d":`)); err != nil {
				return err
			}
			if err := writer.Flush(); err != nil {
				return err
			}
			return conn.WriteMessage(websocket.BinaryMessage, compressed.Bytes())
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gateway, stop := newHandshakeTestGateway(t, tc.exchange)
			session := newHandshakeTestSession(t, gateway)
			openReturned := false
			t.Cleanup(func() {
				stop()
				if openReturned {
					_ = session.Close()
				}
			})
			opened := make(chan error, 1)
			go func() { opened <- session.Open() }()
			var err error
			select {
			case err = <-opened:
			case <-time.After(time.Second):
				t.Error("Open did not enforce its handshake timeout")
				stop()
				select {
				case err = <-opened:
				case <-time.After(time.Second):
					t.Fatal("Open remained blocked after peer closed")
				}
			}
			openReturned = true
			var timeout net.Error
			if !errors.As(err, &timeout) || !timeout.Timeout() {
				t.Errorf("Open error = %v, want a timeout", err)
			}
			if session.wsConn != nil {
				t.Error("failed handshake retained its socket")
			}
			if session.listening != nil {
				t.Error("failed handshake started normal listening")
			}

			// The timeout must release locks/resources so another attempt works.
			stop()
			retryGateway, retryStop := newHandshakeTestGateway(t, handshakeTestReady)
			t.Cleanup(func() {
				retryStop()
				_ = session.Close()
			})
			session.SetGatewayURL(retryGateway)
			session.GatewayHandshakeTimeout = time.Second
			if err := session.Open(); err != nil {
				t.Fatalf("retry after handshake timeout: %v", err)
			}
		})
	}
}

func TestGatewayHandshakeDeadlineCleared(t *testing.T) {
	results := make(chan error, 1)
	gateway, stop := newHandshakeTestGateway(t, func(conn *websocket.Conn) error {
		err := func() error {
			if err := handshakeTestReady(conn); err != nil {
				return err
			}
			var heartbeat handshakeTestFrame
			if err := conn.ReadJSON(&heartbeat); err != nil {
				return err
			}
			if err := conn.WriteJSON(map[string]any{"op": 11, "d": nil}); err != nil {
				return err
			}
			// Both the read and write deadline would have expired by now.
			time.Sleep(500 * time.Millisecond)
			if err := conn.WriteJSON(map[string]any{"op": 1, "d": nil}); err != nil {
				return err
			}
			if err := conn.ReadJSON(&heartbeat); err != nil {
				return fmt.Errorf("heartbeat after handshake deadline: %w", err)
			}
			if heartbeat.Op != 1 {
				return fmt.Errorf("heartbeat opcode = %d, want 1", heartbeat.Op)
			}
			return nil
		}()
		results <- err
		return nil
	})
	session := newHandshakeTestSession(t, gateway)
	t.Cleanup(func() {
		stop()
		_ = session.Close()
	})
	if err := session.Open(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-results:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no heartbeat after the handshake deadline")
	}
}

func TestGatewayHandshakeDefaultTimeout(t *testing.T) {
	session, err := New("Bot handshake-test")
	if err != nil {
		t.Fatal(err)
	}
	if session.GatewayHandshakeTimeout != 60*time.Second {
		t.Errorf("default timeout = %v, want 60s", session.GatewayHandshakeTimeout)
	}
}

func TestGatewayHandshakeCompressedDeadlineCleared(t *testing.T) {
	gateway, stop := newHandshakeTestGateway(t, func(conn *websocket.Conn) error {
		var compressed bytes.Buffer
		writer := zlib.NewWriter(&compressed)
		defer writer.Close()
		encoder := json.NewEncoder(writer)
		send := func(frame any) error {
			if err := encoder.Encode(frame); err != nil {
				return err
			}
			if err := writer.Flush(); err != nil {
				return err
			}
			data := append([]byte(nil), compressed.Bytes()...)
			compressed.Reset()
			return conn.WriteMessage(websocket.BinaryMessage, data)
		}
		if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
			return err
		}
		if err := send(map[string]any{"op": 10, "d": map[string]any{"heartbeat_interval": 3_600_000}}); err != nil {
			return err
		}
		var identify handshakeTestFrame
		if err := conn.ReadJSON(&identify); err != nil {
			return err
		}
		if identify.Op != 2 {
			return fmt.Errorf("IDENTIFY opcode = %d, want 2", identify.Op)
		}
		if err := send(map[string]any{
			"op": 0, "t": "READY", "s": 42,
			"d": map[string]any{
				"v": 9, "session_id": "compressed-handshake-test",
				"user":   map[string]any{"id": "1", "username": "handshake-test", "bot": true},
				"guilds": []any{}, "private_channels": []any{},
			},
		}); err != nil {
			return err
		}
		time.Sleep(500 * time.Millisecond)
		return send(map[string]any{"op": 0, "t": "RESUMED", "s": 43, "d": map[string]any{}})
	})
	session := newHandshakeTestSession(t, gateway)
	resumed := make(chan struct{}, 1)
	session.EventHandler = func(evt any) {
		if _, ok := evt.(*Resumed); ok {
			resumed <- struct{}{}
		}
	}
	t.Cleanup(func() {
		stop()
		_ = session.Close()
	})
	if err := session.Open(); err != nil {
		t.Fatalf("compressed handshake: %v", err)
	}
	select {
	case <-resumed:
	case <-time.After(2 * time.Second):
		t.Fatal("compressed stream did not remain usable after handshake deadline")
	}
}
