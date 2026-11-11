# Delivery queue example

This integration example admits JetStream messages into a MySQL outbox and delivers them to HTTP
receivers. It uses duplicate admission, independently scoped endpoints, a retry hint, exhaustion,
manual requeue, and cleanup. It acknowledges each input message after the database transaction commits.
Delivery remains at least once, and the receiver gets the outbox ID on each attempt.

Run it with Docker available:

```sh
GOWORK=off go test -tags=integration -count=1 ./...
```

The test owns its MySQL and NATS containers and its local HTTP server. The dependencies are pinned in
this module. Run the command from this directory after the coordinated library release is available.
