//go:build integration

package delivery_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"

	"github.com/velmie/outbox"
	"github.com/velmie/outbox/mysql"
)

func TestDeliveryQueueEndToEnd(t *testing.T) {
	ctx := context.Background()
	db := startMySQLContainer(t, ctx)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})
	ddl, err := mysql.SchemaWithOptions("outbox", mysql.SchemaOptions{
		BinaryPayload:      true,
		Retry:              true,
		AggregateTypeIndex: true,
		Deduplication:      true,
	})
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, ddl)
	require.NoError(t, err)
	producer, err := mysql.NewStore(db, mysql.WithValidatePayload(false))
	require.NoError(t, err)
	entry := outbox.Entry{
		AggregateType: "first",
		AggregateID:   "route",
		EventType:     "delivery",
		Payload:       []byte("not JSON"),
		DedupKey:      admissionKey("route", "message"),
	}
	js := startNATS(t, ctx)
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "DELIVERY",
		Subjects: []string{"deliveries"},
	})
	require.NoError(t, err)
	subscription, err := js.PullSubscribe("deliveries", "admission")
	require.NoError(t, err)
	for i := 0; i < 2; i++ {
		_, err = js.Publish("deliveries", []byte("message"))
		require.NoError(t, err)
	}
	var id outbox.ID
	for i := 0; i < 2; i++ {
		messages, fetchErr := subscription.Fetch(1, nats.MaxWait(10*time.Second))
		require.NoError(t, fetchErr)
		require.Len(t, messages, 1)
		entry.DedupKey = admissionKey("route", string(messages[0].Data))
		tx, beginErr := db.BeginTx(ctx, nil)
		require.NoError(t, beginErr)
		inserted, insertErr := producer.Enqueue(ctx, tx, entry)
		if i == 0 {
			require.NoError(t, insertErr)
			id = inserted
		} else {
			require.ErrorIs(t, insertErr, outbox.ErrDuplicate)
			require.True(t, inserted.IsZero())
		}
		require.NoError(t, tx.Commit())
		require.NoError(t, messages[0].AckSync())
	}
	entry.AggregateType = "second"
	entry.DedupKey = admissionKey("another-route", "message")
	secondID, err := producer.Enqueue(ctx, db, entry)
	require.NoError(t, err)

	entered := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	var firstCalls atomic.Int32
	var receivedMu sync.Mutex
	received := make(map[string][]string)
	var receiverErrors []error
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		body, readErr := io.ReadAll(req.Body)
		receivedMu.Lock()
		received[req.URL.Path] = append(received[req.URL.Path], req.Header.Get("Delivery-ID"))
		if readErr != nil {
			receiverErrors = append(receiverErrors, readErr)
		} else if string(body) != "not JSON" {
			receiverErrors = append(receiverErrors, errors.New("receiver got an unexpected payload"))
		}
		receivedMu.Unlock()
		if req.URL.Path == "/first" {
			call := firstCalls.Add(1)
			if call == 1 {
				close(entered)
				<-release
			}
			if call <= 2 {
				w.Header().Set("Retry-After", "3600")
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(release) })
		receiver.Close()
	})
	newRelay := func(name string) (*mysql.Store, *outbox.Relay) {
		endpoint, createErr := mysql.NewStore(
			db,
			mysql.WithAggregateType(name),
			mysql.WithRetrySchedule(func(int) time.Duration { return 0 }),
			mysql.WithMaxAttempts(2),
		)
		require.NoError(t, createErr)
		handler := outbox.HandlerFunc(func(ctx context.Context, record outbox.Record) error {
			req, requestErr := http.NewRequestWithContext(
				ctx,
				http.MethodPost,
				receiver.URL+"/"+name,
				bytes.NewReader(record.Payload),
			)
			if requestErr != nil {
				return requestErr
			}
			req.Header.Set("Delivery-ID", record.ID.String())
			response, requestErr := receiver.Client().Do(req)
			if requestErr != nil {
				return requestErr
			}
			_, readErr := io.Copy(io.Discard, response.Body)
			if closeErr := response.Body.Close(); readErr != nil || closeErr != nil {
				return fmt.Errorf("read receiver response: %w", errors.Join(readErr, closeErr))
			}
			if response.StatusCode >= 500 {
				seconds, parseErr := strconv.Atoi(response.Header.Get("Retry-After"))
				if parseErr != nil {
					return fmt.Errorf("receiver returned %s", response.Status)
				}
				return outbox.RetryAfter(fmt.Errorf("receiver returned %s", response.Status), time.Duration(seconds)*time.Second)
			}
			if response.StatusCode < 200 || response.StatusCode >= 300 {
				return fmt.Errorf("receiver returned %s", response.Status)
			}
			return nil
		})
		return endpoint, outbox.NewRelay(endpoint, handler, outbox.WithBatchSize(1))
	}
	firstStore, firstRelay := newRelay("first")
	_, secondRelay := newRelay("second")
	result := make(chan error, 1)
	go func() {
		_, processErr := firstRelay.ProcessOnce(ctx)
		result <- processErr
	}()
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("first endpoint did not receive a request")
	}
	processed, err := secondRelay.ProcessOnce(ctx)
	require.NoError(t, err)
	require.True(t, processed)
	releaseOnce.Do(func() { close(release) })
	require.NoError(t, <-result)
	state := readRetryState(t, ctx, db, id)
	require.Equal(t, outbox.StatusPending, state.status)
	require.True(t, state.deadline.Valid)
	requireNoRetryRecords(t, ctx, firstStore)
	makeRetryDue(t, ctx, db, id)
	processed, err = firstRelay.ProcessOnce(ctx)
	require.NoError(t, err)
	require.True(t, processed)
	require.Equal(t, outbox.StatusDead, readRetryState(t, ctx, db, id).status)
	count, err := firstStore.Requeue(ctx, mysql.RequeueOptions{
		IDs:   []outbox.ID{id},
		Limit: 1,
	})
	require.NoError(t, err)
	require.EqualValues(t, 1, count)
	processed, err = firstRelay.ProcessOnce(ctx)
	require.NoError(t, err)
	require.True(t, processed)
	status, _ := fetchStatus(t, ctx, db, id)
	require.Equal(t, outbox.StatusProcessed, status)
	status, _ = fetchStatus(t, ctx, db, secondID)
	require.Equal(t, outbox.StatusProcessed, status)
	receivedMu.Lock()
	observedErrors := append([]error(nil), receiverErrors...)
	firstIDs := append([]string(nil), received["/first"]...)
	secondIDs := append([]string(nil), received["/second"]...)
	receivedMu.Unlock()
	require.Empty(t, observedErrors)
	require.Equal(t, []string{id.String(), id.String(), id.String()}, firstIDs)
	require.Equal(t, []string{secondID.String()}, secondIDs)
	cleaned, err := producer.Cleanup(ctx, mysql.CleanupOptions{
		Before: time.Now().Add(time.Hour),
	})
	require.NoError(t, err)
	require.EqualValues(t, 2, cleaned.Processed)
	require.Zero(t, cleaned.Dead)
}
