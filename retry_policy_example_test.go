package outbox_test

import (
	"errors"
	"fmt"
	"time"

	"github.com/velmie/outbox"
)

func ExampleRetryAfter() {
	cause := errors.New("service unavailable")
	err := outbox.RetryAfter(cause, 30*time.Second)
	fmt.Println(err)
	fmt.Println(errors.Is(err, cause))
	// Output:
	// service unavailable
	// true
}

func ExampleWithErrorDetails() {
	var config outbox.RelayConfig
	outbox.WithErrorDetails()(&config)
	fmt.Println(config.ErrorDetails)
	// Output: true
}
