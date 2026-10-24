package discordgo

import (
	"errors"
	"github.com/gorilla/websocket"
	"io"
	"net"
)

// logGatewayDiagnostic omits peer text, URLs, tokens and event payloads.
func (s *Session) logGatewayDiagnostic(kind string, err error) {
	code, class := 0, "none"
	if err != nil {
		class = "other"
		var closeErr *websocket.CloseError
		var networkErr net.Error
		switch {
		case errors.As(err, &closeErr):
			code, class = closeErr.Code, "websocket_close"
		case errors.As(err, &networkErr):
			class = "transport"
			if networkErr.Timeout() {
				class = "timeout"
			}
		case errors.Is(err, io.EOF):
			class = "eof"
		}
	}
	level := LogWarning
	switch kind {
	case "server_reconnect":
		level = LogInformational
	case "connect_error", "heartbeat_write_error", "heartbeat_ack_timeout":
		level = LogError
	}
	s.log(level, "gateway_diagnostic kind=%s close_code=%d error_class=%s", kind, code, class)
}
