## 3.0.0-alpha.3

- A job with no fields no longer gives an "unused variable" warning in `lib/generated/jobs.g.dart`.

## 3.0.0-alpha.2

- The builder now writes the job registry to `lib/generated/jobs.g.dart`. Import it from there.

## 3.0.0-alpha.1

- First version.
- Jobs: `@Job()` and `DoxJob` with `tries`, `backoff`, `timeout`, `queue` and a `failed()` hook.
- A builder writes `lib/jobs.g.dart`: it saves and loads the constructor parameters of each job, with no mirrors.
- `dispatch()`, `dispatchSync()` and `Queue.dispatch()` with `delay` and `queue`.
- Drivers: `sync`, `database` (Postgres, MySQL, MariaDB, SQLite) and `redis`.
- `QueueService` sets up the queue in every isolate. `workInProcess` runs a worker in the web server.
- `QueueWorker` for `bin/queue.dart`: `work`, `failed`, `retry`, `forget` and `flush`.
- Retries with backoff, failed jobs, timeouts, delayed jobs, crashed worker recovery and graceful stop.
- `QueueTables` makes the `jobs` and `failed_jobs` tables.
