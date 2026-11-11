package mysql_test

import (
	"fmt"
	"time"

	"github.com/velmie/outbox/mysql"
)

func ExampleWithRetrySchedule() {
	var config mysql.Config
	mysql.WithRetrySchedule(func(attempt int) time.Duration {
		return time.Duration(attempt) * time.Second
	})(&config)
	fmt.Println(config.RetrySchedule(2))
	// Output: 2s
}
