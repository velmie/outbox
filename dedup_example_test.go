package outbox_test

import (
	"fmt"

	"github.com/velmie/outbox"
)

func ExampleEntry_deduplication() {
	entry := outbox.Entry{
		AggregateType: "endpoint-a",
		EventType:     "delivery",
		Payload:       []byte(`{"message":"hello"}`),
		DedupKey:      "route-and-message-identity",
	}
	fmt.Println(entry.Validate())
	// Output: <nil>
}
