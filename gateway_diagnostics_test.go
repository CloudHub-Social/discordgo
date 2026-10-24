package discordgo

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

type diagnosticLogs struct {
	mu    sync.Mutex
	lines []string
}

func (l *diagnosticLogs) write(level, caller int, format string, args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines = append(l.lines, fmt.Sprintf(format, args...))
}
func (l *diagnosticLogs) text() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return strings.Join(l.lines, "\n")
}
func diagnosticSession(t *testing.T) (*Session, *diagnosticLogs) {
	t.Helper()
	s, err := New("Bot local-diagnostic-test")
	if err != nil {
		t.Fatal(err)
	}
	s.ShouldReconnectOnError = false
	s.EventHandler = func(any) {}
	s.LogLevel = LogInformational
	logs := &diagnosticLogs{}
	s.Logger = logs.write
	return s, logs
}
func TestGatewayDiagnosticsReconnect(t *testing.T) {
	for _, handshake := range []bool{false, true} {
		t.Run(fmt.Sprint(handshake), func(t *testing.T) {
			s, logs := diagnosticSession(t)
			_, _ = s.onEvent(websocket.TextMessage, []byte("{\"op\":7,\"d\":null}"), handshake)
			if !strings.Contains(logs.text(), "gateway_diagnostic kind=server_reconnect close_code=0 error_class=none") {
				t.Fatalf("missing payload-free server reconnect diagnostic: %s", logs.text())
			}
		})
	}
}
func TestGatewayHealthHeartbeatAcknowledged(t *testing.T) {
	s, logs := diagnosticSession(t)
	_, err := s.onEvent(websocket.TextMessage, []byte("{\"op\":11,\"d\":null}"), false)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(logs.text(), "gateway_health heartbeat_ack=true") {
		t.Fatal("missing heartbeat health marker at informational level")
	}
	if s.LastHeartbeatAck.IsZero() {
		t.Fatal("heartbeat was not recorded")
	}
}
func TestGatewayDiagnosticsPeerCloseRedactsReason(t *testing.T) {
	gateway, _ := newHandshakeTestGateway(t, func(conn *websocket.Conn) error {
		return conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(4002, "PRIVATE_TEST_REASON"), time.Now().Add(time.Second))
	})
	s, logs := diagnosticSession(t)
	conn, _, err := websocket.DefaultDialer.Dial(gateway, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	s.wsConn = conn
	s.listen(conn, make(chan interface{}))
	got := logs.text()
	if strings.Contains(got, "PRIVATE_TEST_REASON") {
		t.Error("peer-supplied close reason leaked into logs")
	}
	if !strings.Contains(got, "gateway_diagnostic kind=read_error close_code=4002 error_class=websocket_close") {
		t.Errorf("missing classified close diagnostic: %s", got)
	}
}
func TestGatewayDiagnosticsInvalidSession(t *testing.T) {
	gateway, _ := newHandshakeTestGateway(t, func(conn *websocket.Conn) error { return nil })
	s, logs := diagnosticSession(t)
	conn, _, err := websocket.DefaultDialer.Dial(gateway, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	s.wsConn = conn
	_, err = s.onEvent(websocket.TextMessage, []byte("{\"op\":9,\"d\":false}"), false)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(logs.text(), "gateway_diagnostic kind=invalid_session close_code=0 error_class=none") {
		t.Fatal("missing invalid-session diagnostic")
	}
}

func TestGatewayDiagnosticsRetainErrorLevel(t *testing.T) {
	s, logs := diagnosticSession(t)
	s.LogLevel = LogError
	s.logGatewayDiagnostic("heartbeat_write_error", fmt.Errorf("PRIVATE_TEST_REASON"))
	if !strings.Contains(logs.text(), "gateway_diagnostic kind=heartbeat_write_error") {
		t.Fatal("heartbeat errors disappeared when error-only logging is configured")
	}
	if strings.Contains(logs.text(), "PRIVATE_TEST_REASON") {
		t.Fatal("error details leaked")
	}
}
