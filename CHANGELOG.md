## Unreleased

- `SendQueuedMail` and `SendEncryptedQueuedMail` are now exported. A test with `Queue.fake()` can check a queued email with `queue.assertPushed<SendQueuedMail>()`.
- Docs: `clear` also deletes the reserved (running) jobs of the queue, not only the waiting and delayed ones.
- Note for custom drivers: a driver that `extends QueueDriver` gets `clear()` and `pruneFailed()` (they throw an `UnsupportedError`). A driver that `implements QueueDriver` gets no code from it, so it must add `clear()` and `pruneFailed()` itself, or it does not compile.

## 3.1.0

- New opt-in job settings: `retryUntil` (retry until a time, not `tries` times), `maxExceptions` (fail after this many tries that threw) and `failOnTimeout` (fail at once on a timeout). Jobs without them work as before. `retryUntil` is read once at dispatch and saved with the job. Rows saved before this change are read as jobs with no `retryUntil`. `queue:retry` gives a failed job a fresh start: the tries and the exceptions count go back to 0, and `retryUntil` is read again from the job class.
- New: `dispatch(delayUntil: DateTime)` waits until a time. `delay: Duration` still works. Give one of them, not both.
- New: `dispatchIf(condition)` and `dispatchUnless(condition)`. They return the job id, or null when the job was not dispatched.
- New worker flags: `--backoff=SECONDS[,SECONDS...]` (the retry wait of a job with no `backoff`), `--max-jobs=N`, `--max-time=SECONDS` and `--memory=MB` (stop the worker after the running jobs). All are off by default.
- New commands of `bin/queue.dart`: `clear [queue]` deletes the jobs of a queue. `restart` makes the running workers stop after their current jobs (it writes a time to the app cache, so the workers must share one cache store). `prune-failed --hours=N` deletes failed jobs older than N hours (default 24).
- New `QueueDriver.clear()` and `QueueDriver.pruneFailed()`. The Redis and database drivers have them. A custom driver that does not override them throws an `UnsupportedError`.
- `QueueDriver.release()` now also saves `job.payload`, so the worker can keep the exception count. A custom driver should save it too.
- Fix: the `QueueFake` messages said "a UnusedJob". Now only "Uni" and "U" + one consonant (not "n") + vowel give "a", so it says "an UnusedJob" and still "a UniJob".
- Fix: when `Mail.fake()` was on before `Queue.fake()`, `Queue.restore()` lost a queue set with `Mail.useQueue()`. Now `Queue.fake()` does not touch the mail queue while `Mail.fake()` is on.

- Fix: the `QueueFake` check messages had bad grammar, like "a AllTypesJob" and "1 times". Now they say "an AllTypesJob" and "1 time". A name that starts with a "you" sound (like `UserJob` or `EuJob`) gets "a".
- Fix: `Queue.restore()` did not undo the `Mail.useQueue()` call of `Queue.fake()`. With no `Queue.configure()`, `Mail.queue()` still found a queue after the test. Now `restore()` puts back the queue it had before `Queue.fake()` (also one from `Mail.useQueue()`). If it had none: no queue, or this queue when `Queue.configure()` was called.
- New: `Queue.fake()` for tests. It keeps every dispatched job in a `QueueFake` and does not run or save them. Check them with `assertPushed<T>([test])`, `assertPushedTimes<T>(n)`, `assertNotPushed<T>()` and `assertNothingPushed()`. Read them with `pushed` (each has the job, the queue name and the delay) or `jobsOf<T>()`. It also catches `Mail.queue()` and `Mail.later()`. `dispatchSync()` still runs the job. `Queue.restore()` stops the fake.
- Fix: with `Mail.queue(message, encrypt: true)`, a send error could name the recipient (eg. an SMTP reply `550 <ann@example.com>: mailbox unavailable`). The worker wrote that error to its log and to the failed jobs in plain text. Now the encrypted mail job throws a `RedactedMailError`. Its text has only the error type and the SMTP or HTTP code. The full error text, with the answer of the server, is encrypted with the app key (at most the first 64K characters, counted in UTF-16 code units). Read it with `RedactedMailError.decrypt(job.error)`. When the error can not be encrypted, only the type and the code are kept. Mail jobs without `encrypt` keep the full error, as before.
- New: `dox queue:table` writes a migration for the `jobs` and `failed_jobs` tables in `db/migration`. It calls `QueueTables.up` and `QueueTables.down`. It does not overwrite: when a `create_queue_tables` migration is already there, it prints so and exits with code 1. It is a package command (`dox: commands:` in pubspec.yaml), so it needs a `dox` CLI that finds package commands.
- Needs dox_core with `package:dox_core/command.dart`.
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
