package mysql

import "fmt"

type queries struct {
	insert           string
	selectPending    string
	updateFailureOne string
	updateDeadOne    string
	countPending     string
}

func newQueries(table string, retry bool) queries {
	eligibility := ""
	deadline := ""
	if retry {
		eligibility = " AND (next_attempt_at IS NULL OR next_attempt_at <= UTC_TIMESTAMP(6))"
		deadline = ", cur.next_attempt_at = TIMESTAMPADD(MICROSECOND, ?, UTC_TIMESTAMP(6))"
	}

	cols := "id, aggregate_type, aggregate_id, event_type, payload, headers, created_at, attempt_count"
	insert := fmt.Sprintf("INSERT INTO %s (id, aggregate_type, aggregate_id, event_type, payload, headers) VALUES (?, ?, ?, ?, ?, ?)", table)
	selectBase := fmt.Sprintf(
		"SELECT %s FROM %s WHERE status = ?%s ORDER BY id ASC LIMIT ? FOR UPDATE SKIP LOCKED",
		cols,
		table,
		eligibility,
	)
	updateFailureOne := fmt.Sprintf(
		"UPDATE %s AS cur "+
			"JOIN %s AS prev ON prev.id = cur.id "+
			"SET cur.attempt_count = prev.attempt_count + 1, cur.last_error = ?, "+
			"cur.status = CASE WHEN (prev.attempt_count + 1) >= ? THEN ? ELSE ? END%s "+
			"WHERE cur.id = ?",
		table,
		table,
		deadline,
	)
	updateDeadOne := fmt.Sprintf(
		"UPDATE %s SET attempt_count = attempt_count + 1, last_error = ?, status = ? WHERE id = ?",
		table,
	)
	countPending := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE status = ?", table)

	return queries{
		insert:           insert,
		selectPending:    selectBase,
		updateFailureOne: updateFailureOne,
		updateDeadOne:    updateDeadOne,
		countPending:     countPending,
	}
}
