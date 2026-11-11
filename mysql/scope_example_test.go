package mysql_test

import (
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"

	"github.com/velmie/outbox/mysql"
)

func ExampleWithAggregateType() {
	db, err := sql.Open("mysql", "user:password@tcp(localhost:3306)/database")
	if err != nil {
		panic(err)
	}
	defer db.Close()
	store, err := mysql.NewStore(db, mysql.WithAggregateType("endpoint-a"))
	if err != nil {
		panic(err)
	}
	fmt.Println(store != nil)
	// Output: true
}
