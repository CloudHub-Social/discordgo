package discordgo

import (
	"errors"
	"testing"
	"time"
)

type fakeCallSnapshotWriter struct {
	deadlines    []time.Time
	failDeadline bool
	failWrite    bool
	closed       bool
	data         interface{}
}

func (f *fakeCallSnapshotWriter) SetWriteDeadline(t time.Time) error {
	f.deadlines = append(f.deadlines, t)
	if f.failDeadline {
		return errors.New("deadline")
	}
	return nil
}
func (f *fakeCallSnapshotWriter) WriteJSON(v interface{}) error {
	f.data = v
	if f.failWrite {
		return errors.New("write")
	}
	return nil
}
func (f *fakeCallSnapshotWriter) Close() error { f.closed = true; return nil }
func TestCallSnapshotWriteDeadline(t *testing.T) {
	f := &fakeCallSnapshotWriter{}
	before := time.Now()
	if err := writeCallSnapshotRequest(f, "123"); err != nil {
		t.Fatal(err)
	}
	if len(f.deadlines) != 2 || f.deadlines[0].Before(before.Add(9*time.Second)) || !f.deadlines[1].IsZero() || f.closed {
		t.Fatalf("unexpected deadlines or closure: %+v", f)
	}
	if op, ok := f.data.(markViewingOp); !ok || op.Op != 13 || op.Data.ChannelID != "123" {
		t.Fatalf("unexpected request: %#v", f.data)
	}
	for _, fail := range []string{"deadline", "write"} {
		f = &fakeCallSnapshotWriter{failDeadline: fail == "deadline", failWrite: fail == "write"}
		if err := writeCallSnapshotRequest(f, "123"); err == nil || !f.closed {
			t.Fatalf("%s: expected error and closed connection", fail)
		}
	}
}
