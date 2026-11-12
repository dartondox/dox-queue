# Config options and drivers

## QueueConfig options

| Option | Default | What it does |
| --- | --- | --- |
| `driver` | `sync` | `sync`, `database` or `redis` |
| `jobs` | empty | `jobRegistry` from the generated file |
| `redis` | localhost:6379 | `RedisOptions(host:, port:, password:)` or `QueueConfig.redisFromEnv()` |
| `prefix` | `dox_queue:` | start of every Redis key |
| `connection` | default | dox_orm connection name for the `database` driver |
| `retryAfter` | 90 seconds | a job not finished in this time can be taken by another worker |
| `workInProcess` | `false` | run a worker inside the web server (small apps only) |
| `workerQueues`, `workerConcurrency` | `['default']`, `1` | settings of that in-process worker |
| `serializeSync` | `false` | `sync` driver saves and reloads the job before it runs |
| `connections` | none | more queue connections by name |
| `afterCommit` | `false` | push jobs from inside a transaction only after the commit |
| `failedAtTimestamp` | `false` | `failed_jobs.failed_at` as a timestamp, not a number |


## Redis from the env

```dart
QueueConfig queueConfig = QueueConfig(
  driver: 'redis',
  jobs: jobRegistry,
  redis: QueueConfig.redisFromEnv(),
);
```

`redisFromEnv()` reads `REDIS_URL` (like `rediss://user:password@host:6380/0`; `rediss://` turns on TLS). Else it reads `REDIS_HOST`, `REDIS_PORT`, `REDIS_USERNAME`, `REDIS_PASSWORD` and `REDIS_TLS`. With no `redis:` option, the queue uses localhost:6379 and does not read the env.

## Database driver

- Postgres, MySQL 8+, MariaDB 10.6+ and SQLite work. Many workers do not wait on each other (`SKIP LOCKED`). SQLite is good for small apps.
- Use another dox_orm connection with `QueueConfig(connection: 'postgres')`.
- `failed_jobs.failed_at` is a number (unix milliseconds) by default. For a timestamp column use `failedAtTimestamp: true` in the config and `QueueTables.up(schema, failedAtTimestamp: true)` in the migration. The setting and the column must match, or a failed job is not moved to `failed_jobs` and runs again after `retryAfter`.
- Tables by hand: `QueueTables.up(schema)`, `QueueTables.upBatches(schema)`, and the matching `down` and `downBatches`.

## Sync driver

`dispatch()` runs the job at once and waits. No worker, no retry, no delay. A throwing job runs `failed()` and `dispatch()` throws.
`serializeSync: true` saves and reloads the job first, the way a worker would. Then a job that is not in `jobRegistry` throws at `dispatch()`.

## Queue connections

```dart
QueueConfig queueConfig = QueueConfig(
  driver: 'redis',
  jobs: jobRegistry,
  connections: <String, QueueConfig>{
    'database': QueueConfig(driver: 'database'),
  },
);

await SendReport(reportId: 1).dispatch(connection: 'database');
```

Run a worker per connection: `dart run bin/queue.dart work --connection=database`. An unknown name throws `ArgumentError`.
An entry uses only `driver`, `redis`, `prefix`, `connection`, `retryAfter` and `serializeSync`. The `jobs` of the main config count for all.

## Worker inside the web server

`QueueConfig(driver: 'database', jobs: jobRegistry, workInProcess: true)` runs jobs in the first isolate of the web server. Heavy jobs slow the API. Use a worker process for heavy or many jobs.

## Queue events

`Queue.queued`, `Queue.before`, `Queue.after`, `Queue.released`, `Queue.exceptionOccurred`, `Queue.failing`, `Queue.looping`, `Queue.stopping` take a listener `(QueueEvent event)`. Listeners are per isolate: add them in a provider. The sync driver and `Queue.fake()` fire no events.

Batch events take the same listener. `event.batch` is the batch after the step:

```dart
Queue.batchFinished((QueueEvent event) => print('${event.batch!.name} done'));
```

`Queue.batchDispatched` (dispatcher), `Queue.batchProgress`, `Queue.batchFailed` (has `event.error`), `Queue.batchCancelled`, `Queue.batchFinished` (worker). With the sync driver all fire in the dispatcher. `Queue.fake()` fires none.
