package fsm

import (
	"fmt"
	"log/slog"
	"testing"

	fsmv1 "github.com/ampbase-io/fsm/gen/fsm/v1"

	"github.com/oklog/ulid/v2"
)

// BenchmarkMailbox times one signal's full cycle as a transition sees it — the attempt begins, the
// signal is offered, a handler receives it, the COMPLETE records and consumes it — with a backlog of
// unread signals of another name in the mailbox.
func BenchmarkMailbox(b *testing.B) {
	for _, backlog := range []int{0, 10, 100, 1000} {
		b.Run(fmt.Sprintf("backlog=%d", backlog), func(b *testing.B) {
			resume := NewSignal[command]("resume")
			accepted := map[string]AnySignal{"pause": testPause, "advance": testAdvance, "resume": resume}
			mb := newMailbox(accepted, nil, slog.New(slog.DiscardHandler))
			defer mb.close()

			payload, err := testAdvance.codec.Marshal(&command{Stage: 1, Note: "go"})
			if err != nil {
				b.Fatal(err)
			}
			unread := make([]*fsmv1.Signal, backlog)
			for i := range unread {
				unread[i] = &fsmv1.Signal{Id: ulid.Make().String(), Name: "pause", Payload: payload}
			}
			mb.offer(unread...)

			o, _ := mb.outlet("advance")
			ch := o.(*outlet[command]).ch

			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				mb.beginAttempt()
				mb.offer(&fsmv1.Signal{Id: ulid.Make().String(), Name: "advance", Payload: payload})
				<-ch
				mb.consume(mb.received())
			}
		})
	}
}
