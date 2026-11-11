## Unreleased

- New opt-in `QueueConfig(failedAtTimestamp: true)` and `QueueTables.up(schema, failedAtTimestamp: true)`: `failed_jobs.failed_at` is a timestamp column in UTC, like Laravel, not a big integer of unix time in milliseconds. The setting counts per queue connection. `FailedJob.failedAt` is the same `DateTime` with both types. The `database` driver reads the column type once, on its first use of `failed_jobs`, and throws a `StateError` when the setting and the column do not match, so no wrong value is written. When the type can not be read, it is read again on the next call: with the setting off the call goes on after a warning, with the setting on it throws. On MySQL and MariaDB the time is written with `FROM_UNIXTIME()` and read with `UNIX_TIMESTAMP()`, so the session time zone does not change it. The docs have the SQL to change a table with the integer column on SQLite, Postgres and MySQL. With the setting off, the table and the values are as before. The Redis driver does not use it.
- New opt-in `dispatch(afterCommit: true)` (also on `dispatchIf()`, `dispatchUnless()` and `Queue.dispatch()`) and `QueueConfig(afterCommit: true)` for every dispatch, like Laravel `afterCommit()`. Inside a transaction of the default dox_orm connection, the job is pushed only after the outermost transaction commits. A rollback drops it. Outside a transaction it is pushed at once. `afterCommit: false` on one dispatch wins over the setting, like Laravel `beforeCommit()`. `dispatch()` returns the job id at once. A job that is not in the registry, or data that is not JSON, still throws at `dispatch()`. The `sync` driver runs the job after the commit. `dispatchSync()` and `dispatchAfterResponse()` do not change. `Queue.fake()` keeps the job at `dispatch()`, like Laravel. With nothing new used, dispatch works as before. Needs the next dox_orm (with `QueryBuilder.afterCommit()`): the dox_orm constraint must go up to that version at release.
- New opt-in mixin `InteractsWithQueue`, like Laravel: `class MyJob extends DoxJob with InteractsWithQueue`. Inside `handle()` the job can call `attempts()`, `release(delay)`, `fail(error)` and `delete()`. The worker does the call when `handle()` ends: `release()` puts the job back on its queue (it counts as a try, so set `tries` above 1), `fail()` fails it at once and runs `failed()`, `delete()` deletes it with no `failed()`. The last call wins, and a call wins over an error thrown after it (also a `TimeoutException` of the job itself, and in `dispatchSync()` a `fail()` call gives its error to `failed()`, while the thrown error still reaches the caller). When the worker timeout fires, the call is dropped and the timeout rules count. Outside a worker (`dispatchSync()`, `dispatchAfterResponse()`, the `sync` driver) `attempts()` is 1, `release()` and `delete()` do nothing, and `fail()` only runs `failed()`. Jobs without the mixin work as before, also when they have their own members of these names.
- New opt-in `QueueConfig(serializeSync: true)`: the `sync` driver saves the job and reads it back before it runs, like a worker gets it. So a job that is not in the registry, or a field that is not JSON, throws in tests. The default runs the job object as it is.
- New opt-in queue connections: `QueueConfig(connections: {'database': QueueConfig(driver: 'database')})`, `dispatch(connection: 'database')` (also on `dispatchIf()`, `dispatchUnless()` and `Queue.dispatch()`), `Queue.driverOf(name)` and the `--connection=NAME` flag for every command of `bin/queue.dart`. The config itself is the main connection, used when no connection is named. `Queue.fake()` keeps the name in `PushedJob.connection`. A config with no `connections` works as before.
- New: `failed_jobs` has a nullable `connection` column. A failed job of a named connection saves the name there. The main connection leaves it NULL. A `failed_jobs` table without the column keeps working: the driver checks for the column once and then leaves it out. Add it with `table.string('connection').nullable()` in a new migration, then restart the workers (they check for the column once).
- `Queue.fake()` did not keep the jobs of `dispatchSync()` and `dispatchAfterResponse()`, so a test could not check them. Now the fake keeps them in `dispatchedSync` and `dispatchedAfterResponse`. The jobs still run, as before. They are not in `pushed`, so `assertPushed()` and the other checks work as before. `clear()` also empties the two new lists.
- New: `dispatchAfterResponse()` runs a job after the response is sent, like Laravel. The client does not wait for it. The job runs in the same process, like `dispatchSync()`. It does not go on the queue, so it returns nothing (no job id) and has no retry. Outside a request it runs soon. An error goes to the exception handler, never to the client. `QueueProvider` now calls `AfterResponse.enable()` of dox_core for it. Needs dox_core with `AfterResponse`.
- `SendQueuedMail` and `SendEncryptedQueuedMail` are now exported. A test with `Queue.fake()` can check a queued email with `queue.assertPushed<SendQueuedMail>()`.
- Docs: `clear` also deletes the reserved (running) jobs of the queue, not only the waiting and delayed ones.
- Note for custom drivers: a driver that `extends QueueDriver` gets `clear()` and `pruneFailed()` (they throw an `UnsupportedError`). A driver that `implements QueueDriver` gets no code from it, so it must add `clear()` and `pruneFailed()` itself, or it does not compile.
- `Queue.retryUntilOf()` now logs a warning (with the error type) when it can not read the job. Before, it hid every error.
- `JobPayload.retried()` takes the new `retryUntil` as a parameter. The built-in drivers read it with the new `Queue.retriedPayload()`, so `JobPayload` does not read the global `Queue.config` any more. A custom driver that calls `retried()` should use `Queue.retriedPayload()`, or the retried job has no `retryUntil`.

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
- Needs dox_core 3.1.0 or newer.

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
