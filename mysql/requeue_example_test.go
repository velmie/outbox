package mysql_test

import (
	"fmt"

	"github.com/velmie/outbox/mysql"
)

func ExampleRequeueOptions() {
	opts := mysql.RequeueOptions{
		AggregateType: "endpoint-a",
		EventType:     "delivery",
		Limit:         100,
	}
	fmt.Println(opts.AggregateType, opts.EventType, opts.Limit)
	// Output: endpoint-a delivery 100
}
