## Unreleased

- Fix: with `Mail.queue(message, encrypt: true)`, a send error could name the recipient (eg. an SMTP reply `550 <ann@example.com>: mailbox unavailable`). The worker wrote that error to its log and to the failed jobs in plain text. Now the encrypted mail job throws a `RedactedMailError`. It hides every email address, and each whole text of the mail (names, subject, body, header and metadata values, file names) when it is a whole word in the error. A part of a text, an encoded form (JSON, MIME, URL), or another Unicode form (eg. `Zoë` as NFC or NFD) may still show. Header names and metadata keys are not hidden, only their values. The error type and the SMTP or HTTP code stay. When the hiding fails, only the type and the code are kept. Mail jobs without `encrypt` keep the full error, as before.
- New: a built-in job for `Mail.queue()` and `Mail.later()` of dox_mailer. `Queue.configure()` turns it on. Apps do not add it to `lib/generated/jobs.g.dart`: every `JobRegistry` finds the built-in jobs (`JobRegistry.builtIn`) after its own jobs. With `encrypt: true`, the mail is saved like a `ShouldBeEncrypted` job.
- Now depends on dox_mailer.
- Deploy the queue worker together with the web app. An old worker does not know the new mail job, so it fails the queued emails.
- Fix (opt-in): a job that timed out keeps running, and its retry could run at the same time. The new worker flag `--timeout-grace=SECONDS` (`WorkerOptions.timeoutGrace`) makes the worker wait for the old run before the next try. The default does not change. For the in-process worker, use `QueueConfig(workerTimeoutGrace: ...)`.
- New opt-in `QueueConfig.redisFromEnv()`: reads the Redis server from `REDIS_URL`, or from `REDIS_HOST`, `REDIS_PORT`, `REDIS_USERNAME`, `REDIS_PASSWORD` and `REDIS_TLS`. Use it as `QueueConfig(redis: QueueConfig.redisFromEnv())`. Without it, the default is still localhost:6379.
- Opt-in `ShouldBeEncrypted` job mixin: the job data is saved encrypted with the app key (AES-GCM of dox_core). Jobs without it are saved as before.
- Needs dox_core 3.0.2 or newer (for `AppConfig.previousKeys`).

## 3.0.0

- The first stable release.

## 3.0.0-rc.1

- Uses the `Logger` that `dox_core` now exports, so it needs dox_core 3.0.0-rc.1.
- Fix: a `Worker` started without `Dox().initialize()` crashed with a `LateInitializationError` when it logged. Now it logs to stdout, and errors to stderr.

## 3.0.0-alpha.5

- Works with ioredis 3.0.0, so it works with dox_core 3.0.0-alpha.16 and newer.

## 3.0.0-alpha.4

- `QueueProvider` (was a DoxService): extend it in your app, `shutdown()` is optional.

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
