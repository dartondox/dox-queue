# dox_queue

Background jobs for the [Dox](https://dartondox.dev) framework: Redis, database and sync drivers, retries, delays and workers.

A queue runs slow work in the background. The request answers at once. A worker process does the work later.

Use it to send emails, call slow APIs, make reports or resize images.
The `dox_queue` package gives you jobs, three drivers (Redis, database and sync), retries, delays and a worker.

## Install

```bash
dart pub add dox_queue
```

`dox_queue` has a builder. Your app already runs `build_runner` for `dox_builder`, so nothing else is needed.

## How it works

1. You write a job class with `@Job()`.
2. `dart run build_runner build` writes `lib/generated/jobs.g.dart`. It knows how to save and load each job.
3. Your code calls `dispatch()`. The job is saved in Redis or in the database.
4. A worker (`dart run bin/queue.dart`) takes the job and runs its `handle()` method.

## Set up

### 1. Config

`lib/config/queue.dart`:

```dart
import 'package:dox_queue/dox_queue.dart';

import '../generated/jobs.g.dart';

QueueConfig queueConfig = QueueConfig(
  /// sync, database or redis
  driver: 'database',

  /// every @Job class, from lib/generated/jobs.g.dart
  jobs: jobRegistry,
);
```

| Option | Default | What it does |
| --- | --- | --- |
| `driver` | `sync` | `sync`, `database` or `redis` |
| `jobs` | empty | `jobRegistry` from `lib/generated/jobs.g.dart` |
| `redis` | localhost:6379 | `RedisOptions(host: ..., port: ..., password: ...)` for the `redis` driver |
| `prefix` | `dox_queue:` | the start of every Redis key |
| `connection` | default | the dox_orm connection name, for the `database` driver |
| `retryAfter` | 90 seconds | when a worker does not finish a job in this time, another worker can take it |
| `workInProcess` | `false` | run a worker inside the web server, see [Small apps](#small-apps-a-worker-in-the-web-server) |
| `workerQueues` | `['default']` | the queues of the in-process worker |
| `workerConcurrency` | `1` | jobs at once in the in-process worker |
| `workerTimeoutGrace` | off | opt-in: after a timeout, the in-process worker waits up to this long for the old run (see Retries) |
| `serializeSync` | `false` | opt-in: the `sync` driver saves the job and reads it back before it runs, see [Sync](#sync) |
| `connections` | none | opt-in: more queue connections by name, see [Queue connections](#queue-connections) |
| `afterCommit` | `false` | opt-in: push jobs that are dispatched inside a transaction only after it commits, see [Dispatch after the commit](#dispatch-after-the-commit) |

> **Keep `retryAfter` bigger than your longest job**
> `retryAfter` is how Dox finds the job of a crashed worker.
> A job that runs longer than `retryAfter` looks crashed, so a second worker runs it again.

### 2. Provider

The provider sets up the queue in every isolate. Put it in `lib/` so the generated container finds it.

`lib/providers/queue_provider.dart`:

```dart
import 'package:dox_queue/dox_queue.dart';

import '../config/queue.dart';

/// Sets up the queue. Dox finds every DoxProvider class,
/// see lib/container.g.dart.
class AppQueueProvider extends QueueProvider {
  AppQueueProvider() : super(queueConfig);
}
```

### 3. Worker file

`bin/queue.dart`:

```dart
import 'dart:io';

import 'package:dox_queue/dox_queue.dart';
import 'package:my_app/config/app.dart';

/// run queued jobs: `dart run bin/queue.dart [work] [flags]`.
/// the `dox` CLI runs this file for its queue commands.
Future<void> main(List<String> args) async {
  exitCode = await QueueWorker(app: appConfig).run(args);
}
```

The worker starts the providers of your app (database, cache, queue). It does not start the HTTP server.

### 4. Tables (database driver only)

Run `dox queue:table`. It writes a migration in `db/migration` that makes the `jobs` and `failed_jobs` tables, and the `job_batches` table of [job batches](#job-batches). Then run `dox migrate`.

```bash
dox queue:table
dox migrate
```

`queue:table` is a [package command](https://dartondox.dev/digging-deeper/package-commands/) of `dox_queue`. It does not overwrite: when a `create_queue_tables` migration is already there, it prints so and stops.

The migration it writes looks like this. You can also write it by hand. `QueueTables` makes the tables.

`db/migration/2026_10_25_000000_create_queue_tables.dart`:

```dart
// ignore_for_file: file_names
import 'package:dox_migration/dox_migration.dart';
import 'package:dox_queue/dox_queue.dart';

/// The jobs, failed_jobs and job_batches tables of dox_queue.
class CreateQueueTables extends Migration {
  @override
  Future<void> up(Schema schema) async {
    await QueueTables.up(schema);
    await QueueTables.upBatches(schema);
  }

  @override
  Future<void> down(Schema schema) async {
    await QueueTables.downBatches(schema);
    await QueueTables.down(schema);
  }
}
```

`job_batches` is only needed for `Queue.batch()`. An app that ran an older `queue:table` migration adds it in a new migration (see [Job batches](#job-batches)).

## Write a job

`lib/app/jobs/send_welcome_email.job.dart`:

```dart
import 'package:dox_core/dox_core.dart';
import 'package:dox_queue/dox_queue.dart';

@Job()
class SendWelcomeEmail extends DoxJob {
  SendWelcomeEmail({required this.userId});

  final int userId;

  /// run up to 3 times
  @override
  int get tries => 3;

  /// wait 10 seconds before the 2nd try, 60 seconds before the 3rd
  @override
  List<Duration> get backoff => const <Duration>[
    Duration(seconds: 10),
    Duration(seconds: 60),
  ];

  /// stop a try after 30 seconds
  @override
  Duration? get timeout => const Duration(seconds: 30);

  /// the queue of this job
  @override
  String get queue => 'emails';

  @override
  Future<void> handle() async {
    MailService mail = Dox().ioc.get<MailService>();
    await mail.sendWelcome(userId);
  }

  /// runs once, after the last try failed
  @override
  Future<void> failed(Object error, StackTrace stack) async {
    // tell someone, or clean up
  }
}
```

Run `dart run build_runner build` after you add or change a job.

All the settings are optional:

| Setting | Default | What it does |
| --- | --- | --- |
| `tries` | the worker `--tries` (1) | how many times to run the job before it fails |
| `backoff` | none (retry at once) | the wait before each retry. The last entry is used for the rest |
| `timeout` | the worker `--timeout` (60 seconds) | the max run time of one try |
| `retryUntil` | none | retry until this time, not `tries` times. Read once, when the job is dispatched |
| `maxExceptions` | none | fail after this many tries that threw an error. A timeout does not count |
| `failOnTimeout` | `false` | fail at once when a try times out, with no retry |
| `queue` | `default` | the queue to put the job on |
| `failed()` | does nothing | runs once, after the last try failed |

### What is saved

The parameters of the unnamed constructor are saved with the job.
Each parameter needs a field or getter with the same name.

These types work:

- `int`, `double`, `num`, `String`, `bool`
- `DateTime`
- enums (saved by name)
- `List` and `Map<String, ...>` of these, also nested
- `dynamic` and `Object` (the value must be JSON)
- nullable versions of all of them

Any other type stops the build with a clear message.

> **Save an id, not an object**
> Do not save a `User`. Save the `userId`, and load the user in `handle()`.
> The user may change between dispatch and run. The id gives you fresh data.

### Encrypted jobs

Job data is saved as plain JSON in Redis or the `jobs` table.
To hide it from anyone who can read the store, add `ShouldBeEncrypted`:

```dart
@Job()
class ChargeCard extends DoxJob with ShouldBeEncrypted {
  // ...
}
```

The data is then encrypted with AES-GCM and the app key (`AppConfig.appKey`).
The worker needs the same key, or the old key in `AppConfig.previousKeys`.
The job name, queue and id stay plain text.
Jobs without the mixin are saved as before.

### Services in a job

A job is made again from its saved data, so it can not get services in its constructor.
Get them in `handle()` from the Dox container:

```dart
UserService users = Dox().ioc.get<UserService>();
```

The worker runs `registerContainer`, so every `@Inject()` class of your app is there.

### Job names

The class name is saved with each job. When you rename a class, queued jobs of the old name can not be found.
Keep the old name with `@Job(name: 'SendWelcomeEmail')`.

## Dispatch a job

```dart
await SendWelcomeEmail(userId: user.id).dispatch();
```

Both ways do the same:

```dart
await Queue.dispatch(SendWelcomeEmail(userId: user.id));
```

A delay, another queue:

```dart
await SendWelcomeEmail(userId: 1).dispatch(
  delay: Duration(minutes: 10),
  queue: 'emails',
);
```

Wait until a time:

```dart
await SendWelcomeEmail(userId: 1).dispatch(
  delayUntil: DateTime.utc(2030, 1, 1, 9),
);
```

Give `delay` or `delayUntil`, not both.

`dispatch()` returns the job id.

Dispatch only when a condition is true, or false:

```dart
await SendWelcomeEmail(userId: 1).dispatchIf(user.wantsEmail);
await SendWelcomeEmail(userId: 1).dispatchUnless(user.isBlocked);
```

They return null when the job was not dispatched.

Run a job now, in this process, with no queue:

```dart
await SendWelcomeEmail(userId: 1).dispatchSync();
```

Run a job in this process after the response is sent. The client does not wait for it:

```dart
SendWelcomeEmail(userId: 1).dispatchAfterResponse();
```

It does not go on the queue, so there is no job id and no retry. When the server stops, a job that did not run yet is lost.

### Dispatch after the commit

A job that you dispatch inside a database transaction is pushed at once.
A worker can run it before the transaction commits, or after it rolls back.
Then the job reads data that is not saved yet, or data that is never saved.

`afterCommit: true` waits for the commit:

```dart
await QueryBuilder.transaction(() async {
  User user = await User().create(<String, dynamic>{'email': email});
  await SendWelcomeEmail(userId: user.id).dispatch(afterCommit: true);
});
```

- Inside a transaction, the job is pushed after the outermost transaction commits.
- When the transaction rolls back, the job is not pushed.
- Outside a transaction, the job is pushed at once.
- `dispatch()` returns the job id at once. The id is made before the push.
- A job that is not in the registry, or data that is not JSON, throws at `dispatch()`, not at the commit.
- When the push fails after the commit, the `QueryBuilder.transaction()` call throws that error. The data stays saved.
- The `sync` driver runs the job after the commit.
- It waits only for a transaction on the default dox_orm connection, opened with no `connection:` name. A transaction opened by name pushes the job at once, also when the name is the one of the default connection.
- `dispatchIf()`, `dispatchUnless()` and `Queue.dispatch()` take `afterCommit` too.
- `dispatchSync()` and `dispatchAfterResponse()` run the job as before. They do not wait for a commit.

Turn it on for every dispatch with `QueueConfig(afterCommit: true)`.
Then `afterCommit: false` on one dispatch pushes that job at once.

`Queue.fake()` keeps the job when you call `dispatch()`, also when the transaction later rolls back.

It uses `QueryBuilder.afterCommit()` of dox_orm.

### Queue an email

You do not need a job class to send an email later. With [dox_mailer](https://pub.dev/packages/dox_mailer#queue-an-email), call `Mail.queue()`:

```dart
await Mail.queue(message);
await Mail.later(Duration(minutes: 10), message, queue: 'emails');
```

dox_queue has a built-in job for it. It is not in `lib/generated/jobs.g.dart`, and you do not need to add it.

Deploy the worker together with the web app. An old worker does not know this job, so it fails the queued emails.

With `encrypt: true`, a send error is logged and saved as its type and code only, eg. `RedactedMailError: MailException (code 550). Details are encrypted: ...`.
The full error text, with the answer of the server, is encrypted with the app key (at most the first 64K characters, counted in UTF-16 code units). To read it, take a `FailedJob` from `Queue.driver.failed()` and call `RedactedMailError.decrypt(job.error)`.

## Run the worker

```bash
dart run bin/queue.dart
```

Commands:

| Command | What it does |
| --- | --- |
| `work` | run jobs. This is the default |
| `failed` | list the failed jobs |
| `retry <id>` | put one failed job back on its queue |
| `retry all` | put all failed jobs back |
| `forget <id>` | delete one failed job |
| `flush` | delete all failed jobs |
| `clear [queue]` | delete the waiting, delayed and reserved (running) jobs of a queue. Failed jobs stay. It frees the locks of unique jobs; when the process dies in the middle, some locks stay (see Unique jobs). Default: `default` |
| `restart` | stop the running workers after their current jobs |
| `prune-failed` | delete failed jobs older than `--hours=N` (default 24) |

Flags of `work`:

| Flag | Default | What it does |
| --- | --- | --- |
| `--queue=high,default` | `default` | the queues to work. The first one goes first |
| `--concurrency=4` | `1` | jobs at once in this process |
| `--once` | off | run one job, then stop |
| `--tries=3` | `1` | tries of a job that does not set `tries` |
| `--timeout=60` | `60` | max run time in seconds of a job that does not set `timeout` |
| `--sleep=3` | `3` | seconds to wait when the queues are empty |
| `--stop-when-empty` | off | stop when the queues are empty |
| `--timeout-grace=30` | off | after a timeout, wait up to this many seconds for the old run to end before the next try |
| `--backoff=1,5,10` | none | seconds to wait before each retry of a job that does not set `backoff` |
| `--max-jobs=1000` | off | stop after this many jobs |
| `--max-time=3600` | off | stop after this many seconds |
| `--memory=256` | off | stop when the process uses more than this many MB |

With `--max-jobs`, `--max-time` and `--memory`, the worker stops after its running jobs. Let your process manager start it again.

```bash
dart run bin/queue.dart work --queue=emails,default --concurrency=4
```

`--concurrency` runs jobs as async tasks on one thread. It helps for jobs that wait (email, HTTP, database).
For CPU heavy jobs, run more worker processes.

### Stop the worker

Press Ctrl+C, or send SIGTERM (`docker stop` does it).
The worker takes no new job. It waits for the running jobs, then it stops.
A second signal stops it at once.

> **Restart the worker after a deploy**
> The worker keeps your code in memory. Restart it after each deploy, or it runs the old code.

`dart run bin/queue.dart restart` tells every running worker to stop after its current jobs.
Your process manager then starts it with the new code.
It writes a time to the app cache. Each worker reads it every 3 seconds.
So the workers and the command must use the same cache store, eg. `file` on one machine, or `redis`.
The `memory` store does not work for it.

### Keep it running

Run the worker as its own process, next to the web server. For example with Docker Compose:

```yaml
services:
  web:
    build: .
    command: dart run bin/server.dart
  worker:
    build: .
    command: dart run bin/queue.dart work --concurrency=4
```

Or with systemd, Supervisor or a second Fly.io process. Set it to restart when it stops.

## Retries and failed jobs

When `handle()` throws:

1. The worker waits the `backoff` time and tries again, up to `tries` times.
2. After the last try, the job goes to the failed jobs, and `failed()` runs.

A timeout counts as a failed try.

> **A timeout does not stop the code**
> Dart can not stop a running `Future`.
> After a timeout the try counts as failed, but `handle()` keeps running in the background until it ends.
> Write jobs that are safe to run twice.

So by default the next try can start while the old run still works.
Then the job runs twice at the same time.
To stop this, start the worker with `--timeout-grace=SECONDS` (or `WorkerOptions(timeoutGrace: ...)`, or `QueueConfig(workerTimeoutGrace: ...)` for the in-process worker).
After a timeout, the worker then waits for the old run to end, up to that time, before it tries again or fails the job.
The result of the old run is ignored: the try still counts as failed.
While the worker waits, it takes no other job.
If the old run is still going when the grace time ends, the worker logs a warning and goes on.
Keep `timeout` plus the grace time below `retryAfter`, or another worker can take the job again while this one waits. Then the double run still happens.

When a worker crashes, its job stays reserved. After `retryAfter` another worker takes it.
This counts as a try. When there are no tries left, the job fails with a message that the worker did not finish.

Failed jobs are kept until you retry or delete them:

```bash
dart run bin/queue.dart failed
dart run bin/queue.dart retry 6f1c0e...
dart run bin/queue.dart retry all
dart run bin/queue.dart forget 6f1c0e...
dart run bin/queue.dart flush
```

### Control the job in handle()

Add the opt-in `InteractsWithQueue` mixin. Then, inside `handle()`, the job can decide what happens next:

| Call | What it does |
| --- | --- |
| `attempts()` | how many times a worker took the job. The first run gives 1 |
| `release(delay)` | put the job back on its queue. It runs again after `delay` |
| `fail(error)` | fail the job now, with no retry. `failed()` runs |
| `delete()` | delete the job, with no retry and no `failed()` |

```dart
@Job()
class SendReport extends DoxJob with InteractsWithQueue {
  SendReport({required this.reportId});

  final int reportId;

  @override
  int get tries => 5;

  @override
  Future<void> handle() async {
    ReportApi api = Dox().ioc.get<ReportApi>();
    if (await api.isBusy()) {
      release(Duration(seconds: 30));
      return;
    }
    if (!await api.hasReport(reportId)) {
      delete();
      return;
    }
    await api.send(reportId);
  }
}
```

The worker does the call when `handle()` ends. When you call more than one, the last one wins.
A call also wins over an error that `handle()` throws after it.
When `handle()` times out, the call is dropped, and the timeout rules count.

`release()` counts as a try. The default `tries` is 1, so the job then fails on its next pickup.
Set `tries` above 1 (or `retryUntil`) when a job calls `release()`.

With `dispatchSync()`, `dispatchAfterResponse()` and the `sync` driver there is no queue.
There `attempts()` is 1, `release()` and `delete()` do nothing, and `fail()` only runs `failed()`.

In a test file, `package:test` has its own `fail()`. Inside a job class with the mixin, a bare `fail(...)` calls the job method. Write `this.fail(...)` to make it clear.

## Job middleware

Add the opt-in `HasMiddleware` mixin and list the middleware. They wrap `handle()`. The first one runs first.

```dart
@Job()
class SendReport extends DoxJob with HasMiddleware {
  @override
  int get tries => 5;

  @override
  List<JobMiddleware> get middleware => <JobMiddleware>[
    RateLimited('report-api', 10, Duration(minutes: 1)),
    WithoutOverlapping('report-$reportId'),
  ];
  ...
}
```

Write your own with `JobMiddleware`. Call `next()` to go on. Do not call it to skip the job (the job is then done). Call `release(job, delay)` to put it back on the queue.

```dart
class SkipAtNight extends JobMiddleware {
  @override
  Future<void> handle(DoxJob job, JobNext next) async {
    if (DateTime.now().hour < 6) {
      release(job, Duration(hours: 1));
      return;
    }
    await next();
  }
}
```

The built-in middleware:

| Middleware | What it does |
| --- | --- |
| `RateLimited(key, max, per)` | at most `max` jobs of `key` per `per`. The others go back until the window ends |
| `WithoutOverlapping(key, releaseAfter:, expireAfter:)` | one job of `key` at a time. The others go back after `releaseAfter` (null: they are dropped). `expireAfter` frees the lock after a crash |
| `ThrottlesExceptions(key, maxAttempts:, decay:, retryAfter:)` | after `maxAttempts` errors, jobs of `key` wait until `decay` is over. A job that threw goes back after `retryAfter` |

Things to know:

- They keep their counts and locks in the default cache store (`Cache()`). All workers must use the same store: `redis`, or `file` on one machine. A `memory` store is per process.
- A release counts as a try. Give these jobs more `tries`, or a `retryUntil`.
- `WithoutOverlapping` does not add the class name to the lock, unlike Laravel. Job classes with the same key share it.
- With `dispatchSync()`, `dispatchAfterResponse()` and the `sync` driver there is no queue. A middleware that releases the job (eg. `RateLimited` over its limit) skips `handle()`, and a warning is logged.

## Unique jobs

Add the opt-in `ShouldBeUnique` mixin. While a job with the same name and `uniqueId` is on the queue (waiting or running), `dispatch()` pushes nothing and returns an empty id (`''`).

```dart
@Job()
class UpdateSearchIndex extends DoxJob with ShouldBeUnique {
  UpdateSearchIndex({required this.productId});

  final int productId;

  @override
  String get uniqueId => '$productId';

  @override
  Duration? get uniqueFor => Duration(hours: 1);
  ...
}
```

- The worker frees the lock when the job is done, deleted or failed for good. A release or a retry keeps it. `clear` frees the locks of the jobs it deletes.
- `uniqueFor` ends the lock after a time, also when the job never ran. Null: no end.
- `ShouldBeUniqueUntilProcessing` frees the lock when a worker starts the job, so a new one can be queued while it runs.
- The lock is in the default cache store. The web app and the workers must use the same store (eg. `redis`).
- `queue:retry` of a failed job does not take the lock. `dispatchSync()` does not use it.
- With `afterCommit` inside a transaction, the lock is taken at the commit. `dispatch()` returns the id at once, also when the job is then skipped.
- `Queue.fake()` keeps its own locks. `clear()` of the fake frees them.
- When a worker crashes, or `clear` stops in the middle, the locks of those jobs are not freed. With no `uniqueFor` such a lock never ends: `dispatch()` keeps returning `''` until you delete the cache key (`dox_queue:unique:<job name>:<uniqueId>`). So set `uniqueFor` on unique jobs.

## Job chains

Run jobs one after the other, like Laravel `Bus::chain()`. The next job is pushed only when the job before it is done.

```dart
await Queue.chain(<DoxJob>[
  ResizeImage(imageId: 1),
  UploadImage(imageId: 1),
  TellUser(imageId: 1),
]).catchError(TellAdmin(imageId: 1)).dispatch(queue: 'images');
```

- The rest of the chain is saved in the payload of the job that runs now. So it works across worker processes and restarts.
- A job that is released or retried keeps the chain. The chain goes on only after it is done.
- A job that ends with `delete()`, or that a middleware skips, counts as done. The chain goes on.
- When a job fails for good, the rest of the chain is dropped and the catch job runs.
- `queue:retry` of the failed job brings the chain back: when it is then done, the chain goes on.
- `dispatch()` returns the id of the first job. `queue` and `connection` count for every job. `delay` counts for the first job only.

### The catch job

A function can not be saved in the queue. So the catch handler is a job too, of a class in `jobRegistry`. It runs once, in the worker where the job failed, like `dispatchSync()`: no queue, no retry. Add the `CatchesError` mixin to read the error:

```dart
@Job()
class TellAdmin extends DoxJob with CatchesError {
  TellAdmin({required this.imageId});

  final int imageId;

  @override
  Future<void> handle() async {
    await alertAdmin('image $imageId failed: $caughtError');
  }
}
```

An error in the catch job is logged. For slow work, dispatch a new job from it.

### Traps

- Each job carries the data of the jobs after it. Keep chains short, and save ids, not big data, in the jobs.
- `ShouldBeEncrypted` jobs stay encrypted inside the payload of the job before them.
- A `ShouldBeUnique` job in the chain that is skipped (the same job is on the queue) stops the chain. The catch job runs with a `StateError`. When the first job is skipped, `dispatch()` returns `''` and nothing runs.
- A next job that can not be read (eg. its class was removed) stops the chain. The catch job runs with the error.
- The worker pushes the next job before it deletes the job that is done (like Laravel). When the push throws, the try counts as failed: the job is tried again (and runs again), and when no try is left it fails and the catch job runs. A worker that crashes between the push and the delete runs the job again, so the next job can run twice. Make jobs in a chain safe to run twice.
- A failed push counts against the job even though its `handle()` worked: it gets an `exceptionOccurred` event and is released, and at the last try its `failed()` hook and the `failed` event run. With the default `tries` of 1 the first failed push does that, so give chain jobs more than one try.
- The unique lock of a `ShouldBeUnique` job is freed before the next job is pushed, so a next job with the same unique id is not skipped. A retry after a failed push runs without the lock.
- The `sync` driver runs the jobs now, one after the other. When one throws, the catch job runs and `dispatch()` throws the error.
- `Queue.fake()` keeps the chain in `chains`. Its jobs are not in `pushed`.

## Job batches

Run many jobs as one batch, like Laravel `Bus::batch()`, and act when they are all done:

```dart
Batch batch = await Queue.batch(<DoxJob>[
  ImportRows(from: 0),
  ImportRows(from: 1000),
  ImportRows(from: 2000),
])
    .name('import')
    .then(ImportDone())
    .catchError(ImportFailed())
    .whenComplete(CleanUpImport())
    .dispatch(queue: 'imports');

print(batch.id);
```

| Callback | Runs |
| --- | --- |
| `before(job)` | once, at `dispatch()`, before the jobs are pushed |
| `progress(job)` | each time a job is done (with `allowFailures()`, also when one fails) |
| `then(job)` | once, when every job is done |
| `catchError(job)` | once, when the first job fails for good |
| `whenComplete(job)` | once, when every job ran (done or failed), like Laravel `finally` |

A function can not be saved in the queue, so each callback is a job of a class in `jobRegistry`. It runs once, in the process where the step happened (the worker, or the app for `before`), like `dispatchSync()`: no queue, no retry. An error in it is logged. For slow work, dispatch a new job from it. Add `Batchable` to read the batch, and `CatchesError` to read the error:

```dart
@Job()
class ImportFailed extends DoxJob with Batchable, CatchesError {
  @override
  Future<void> handle() async {
    Batch? batch = await this.batch();
    await alertAdmin('import ${batch?.name} failed: $caughtError');
  }
}
```

### The table

Batches are saved in the `job_batches` table of the dox_orm connection of `QueueConfig.connection` (null: the default connection). This is also so with the Redis driver. New apps get it from `dox queue:table`. An app that ran an older `queue:table` migration adds a new one:

```dart
class CreateJobBatchesTable extends Migration {
  @override
  Future<void> up(Schema schema) async {
    await QueueTables.upBatches(schema);
  }

  @override
  Future<void> down(Schema schema) async {
    await QueueTables.downBatches(schema);
  }
}
```

An app that does not use batches does not need the table.

### Jobs of a batch

Add the opt-in `Batchable` mixin to a job to read its batch. `batchId` is the id, `batch()` reads the batch as it is now. A job can skip its work when the batch was cancelled:

```dart
@Job()
class ImportRows extends DoxJob with Batchable {
  ImportRows({required this.from});

  final int from;

  @override
  Future<void> handle() async {
    Batch? batch = await this.batch();
    if (batch?.cancelled ?? false) return;
    await importRows(from);
  }
}
```

`Batch` has `id`, `name`, `totalJobs`, `pendingJobs`, `failedJobs`, `processedJobs`, `progress` (0 to 100), `finished`, `cancelled`, `cancel()` and `add(jobs)`. It is the batch as it was when it was read: read it again with `Queue.findBatch(id)`.

- The first job that fails for good cancels the batch. Add `allowFailures()` to keep it going.
- `cancel()` does not stop the jobs. They still run and count. A `Batchable` job reads `cancelled` to skip its work.
- `add(jobs)` adds jobs, eg. from a job of the batch. They go on the queue and connection of the batch. It throws a `StateError` when the batch is finished, cancelled or deleted, and then pushes nothing.
- `pendingJobs` counts the failed jobs too, as they can be retried (like Laravel). A failed job that you retry with `queue:retry` is marked in its payload. When it fails again, it is not counted again. When it is done, `pendingJobs` and `failedJobs` both go down by one. So `catchError` can run again when a later job fails.
- An empty batch throws an `ArgumentError`: it would never finish.
- Without the `job_batches` table, `dispatch()` throws a `StateError` that names `QueueTables.upBatches()`.
- `finished` is true when every job ran (done or failed), or the batch was cancelled.
- Each count is changed in one atomic step (`UPDATE ... RETURNING` on SQLite and Postgres, a transaction with a row lock on MySQL and MariaDB). So with many workers `then`, `catchError` and `whenComplete` still run once.

### Traps

- A `ShouldBeUnique` job that is skipped (the same job is on the queue) is taken out of `totalJobs` and `pendingJobs`. `progress` does not run for it. When it was the last open job, `then` and `whenComplete` run.
- When a push throws in `dispatch()` or `add()`, the jobs that were not pushed are taken out of the counts, the batch is cancelled (so it is finished and can be pruned), and the error is thrown. No callback runs for those jobs. The jobs pushed before still run, and the callbacks still fire for them (also `then` and `whenComplete`), even though `dispatch()` threw. A `Batchable` job can read `cancelled` to skip its work.
- The counts follow the at-least-once rule of the queue. A job that runs twice (eg. it ran longer than `retryAfter`, or a worker crashed in the middle of a fail) can be counted twice. `failedJobs` never goes below 0.
- The batch is marked finished before `then` and `whenComplete` run, so `add()` from a callback (or from another process at that time) throws a `StateError`.
- A worker that crashes after a job is done, and before it counted it, leaves the batch with that job pending. Then `then` and `whenComplete` never run. `prune-batches --unfinished=N` deletes such batches.
- A chain inside a batch is not supported.
- Batches do not use `afterCommit`: the batch is saved and its jobs are pushed at once.
- The `sync` driver runs the jobs at `dispatch()`, one after the other, and counts them. A job that throws counts as failed: `dispatch()` does not throw. The table is still needed.
- `Queue.fake()` keeps the batch in `batches`, and saves and runs nothing. Its jobs are not in `pushed`. `add()` adds to the faked batch, `cancel()` does nothing, and `Queue.findBatch()` gives null.
- The queue events fire for the jobs of a batch like for any job. There are no batch events.

### Prune old batches

`dart run bin/queue.dart prune-batches --hours=48` deletes the batches that finished (or were cancelled) more than 48 hours ago. The default is 24. Opt-in `--unfinished=168` also deletes the batches that never finished and were made more than 168 hours ago. `Queue.pruneBatches(before, unfinishedBefore:)` does the same in code.

## Queue events

Listen to the steps of a job, like Laravel `Queue::before()`:

```dart
Queue.before((QueueEvent event) => print('start ${event.name} ${event.id}'));
Queue.after((QueueEvent event) => print('done ${event.name}'));
Queue.failing((QueueEvent event) => alert(event.name, event.error));
```

| Hook | When |
| --- | --- |
| `Queue.queued` | `dispatch()` pushed a job |
| `Queue.before` | a worker starts a job |
| `Queue.after` | a worker finished a job and deleted it |
| `Queue.released` | a worker put a job back (`release()` or a retry) |
| `Queue.exceptionOccurred` | `handle()` threw and the job will be tried again |
| `Queue.failing` | a job failed for good |
| `Queue.looping` | a worker loop is about to take a job |
| `Queue.stopping` | a worker stops |

- Listeners are per isolate. Add them where the app starts (eg. in a provider), so the web app and each worker have them.
- A listener that throws is logged. It never stops the worker.
- The `sync` driver, `dispatchSync()`, `dispatchAfterResponse()` and `Queue.fake()` fire no events.
- `Queue.forgetListeners()` removes them all (eg. in tests).

## Schedule a job

With dox_scheduler, `schedule.job()` adds a task that dispatches a job:

```dart
void schedule(Schedule s) {
  s.job(SendDailyReport()).dailyAt('08:00');
  s.job(PruneFiles(), name: 'prune', connection: 'database').hourly();
}
```

The default task name is the class name of the job. Names must be unique in the schedule.

## Drivers

| Driver | Where jobs are | Needs |
| --- | --- | --- |
| `sync` | nowhere: `dispatch()` runs the job at once | nothing |
| `database` | the `jobs` and `failed_jobs` tables | Postgres, MySQL 8+, MariaDB 10.6+ or SQLite |
| `redis` | Redis lists and sorted sets | Redis |

### Database

Jobs are rows in the `jobs` table. Failed jobs are rows in `failed_jobs`.

`failed_jobs.connection` has the name of the queue connection (see [Queue connections](#queue-connections)). It is NULL for the main connection.
A `failed_jobs` table without this column works too. Then the name is not saved. A worker checks for the column once, so restart the workers after you add it.
To add the column, write a migration:

```dart
await schema.table('failed_jobs', (Table table) {
  table.string('connection').nullable();
});
```

- On Postgres, MySQL and MariaDB a worker takes a job with `SELECT ... FOR UPDATE SKIP LOCKED`. Many workers do not wait on each other.
- On SQLite a worker takes a job with one `UPDATE ... RETURNING`. SQLite runs one write at a time, so this is safe. It is good for small apps.

Use another connection of `config/database.dart` with `QueueConfig(connection: 'postgres')`.

#### The failed_at column

`failed_jobs.failed_at` is the time the job failed. It has one of two types:

- A big integer: unix time in milliseconds. This is the default.
- A timestamp in UTC, like Laravel. Turn it on with `failedAtTimestamp: true`.

`FailedJob.failedAt` is the same `DateTime` with both types.

To make a new table with the timestamp column, pass the setting to `QueueTables.up` in the migration, and set it in the queue config too:

```dart
await QueueTables.up(schema, failedAtTimestamp: true);
```

```dart
QueueConfig queueConfig = QueueConfig(
  driver: 'database',
  jobs: jobRegistry,
  failedAtTimestamp: true,
);
```

The setting counts per connection. Each entry of `connections` has its own `failedAtTimestamp`, so two `database` connections can use tables of different types.

The setting and the column must match. A worker reads the column type once, when it first uses `failed_jobs`.

!!! warning "What happens when the setting and the column do not match"
    `fail`, `failed`, `retry`, `forget` and `prune-failed` throw a `StateError`. Nothing is written.

    A job that fails all its tries is NOT moved to `failed_jobs`, and its `failed()` hook does not run. The job stays in `jobs`. It comes back after `retryAfter` and runs again, with an error in the log each time. This goes on until the setting matches the table.

When the type can not be read (for example, the connection dropped), the worker reads it again on the next call. With the setting off, it logs a warning and goes on. With the setting on, the call throws that error, and nothing is written.

The column type on each database:

| Database | Column | How the time is kept |
| --- | --- | --- |
| SQLite | `TIMESTAMP` | ISO text in UTC, with milliseconds |
| Postgres | `TIMESTAMP` (without time zone) | the UTC time, with milliseconds |
| MySQL, MariaDB | `TIMESTAMP` | whole seconds, in UTC. The queue writes it with `FROM_UNIXTIME()` and reads it with `UNIX_TIMESTAMP()`, so the session time zone does not change the time. |

A MySQL and MariaDB `TIMESTAMP` can not hold a time after 2038-01-19.

#### Use the timestamp column on a table with the integer column

!!! warning "Stop all workers first, also on other servers"
    A worker reads the column type when it first uses `failed_jobs`, and keeps it. A worker that is still running writes numbers into the timestamp column. On SQLite this works without an error, and then pruning and the order by time are wrong.

1. Stop all workers, on every server. Stop the app too when it has `workInProcess: true`.
2. Run the SQL for your database below, in a new migration (`await schema.driver.execute('...');` for each line) or in a database client.
3. Set `failedAtTimestamp: true` in the queue config.
4. Start the workers again.

SQLite (3.35 or newer):

```sql
ALTER TABLE failed_jobs ADD COLUMN failed_at_utc TIMESTAMP;
UPDATE failed_jobs SET failed_at_utc = strftime('%Y-%m-%dT%H:%M:%fZ', failed_at / 1000.0, 'unixepoch');
ALTER TABLE failed_jobs DROP COLUMN failed_at;
ALTER TABLE failed_jobs RENAME COLUMN failed_at_utc TO failed_at;
```

Postgres:

```sql
ALTER TABLE failed_jobs ALTER COLUMN failed_at TYPE TIMESTAMP USING to_timestamp(failed_at / 1000.0) AT TIME ZONE 'UTC';
```

MySQL and MariaDB:

```sql
ALTER TABLE failed_jobs ADD COLUMN failed_at_utc TIMESTAMP NULL;
UPDATE failed_jobs SET failed_at_utc = FROM_UNIXTIME(failed_at DIV 1000);
ALTER TABLE failed_jobs DROP COLUMN failed_at;
ALTER TABLE failed_jobs CHANGE failed_at_utc failed_at TIMESTAMP NOT NULL;
```

On SQLite the new column allows NULL, because SQLite can not add a NOT NULL column without a default. The queue never writes NULL there.

### Redis

```dart
QueueConfig queueConfig = QueueConfig(
  driver: 'redis',
  jobs: jobRegistry,
  redis: RedisOptions(host: env.redisHost, port: 6379),
);
```

To read the Redis server from the env (or `.env`), use `QueueConfig.redisFromEnv()`:

```dart
QueueConfig queueConfig = QueueConfig(
  driver: 'redis',
  jobs: jobRegistry,
  redis: QueueConfig.redisFromEnv(),
);
```

It reads:

- `REDIS_URL`, like `rediss://user:password@host:6380/0`. `rediss://` turns on TLS. When it is set, the keys below are not read.
- Else `REDIS_HOST`, `REDIS_PORT`, `REDIS_USERNAME`, `REDIS_PASSWORD` and `REDIS_TLS` (`true` turns on TLS).

A key that is not set keeps its default. With no keys it is localhost:6379.
It is opt-in: when you leave out `redis`, the queue uses localhost:6379 and does not read the env.

Each queue has a list of ready jobs, a sorted set of delayed jobs and a sorted set of reserved jobs.
A Lua script takes a job, so two workers never get the same one. Failed jobs are in the hash `dox_queue:failed`.

### Sync

`dispatch()` runs the job at once and waits for it. There is no worker, no retry and no delay.
When the job throws, `failed()` runs and `dispatch()` throws the error.
Use it for tests and for local work.

By default the sync driver runs the job object as it is.
So a job that is not in `jobRegistry`, or a field that is not JSON, works here and fails later in the worker.
Set `serializeSync: true` to save the job and read it back before it runs, the way a worker gets it:

```dart
Queue.configure(
  QueueConfig(driver: 'sync', jobs: jobRegistry, serializeSync: true),
);
```

Then `dispatch()` throws for such a job.

## Queue connections

One app can use more than one queue store.
The config is the main connection. Add more in `connections`, by name:

```dart
QueueConfig queueConfig = QueueConfig(
  driver: 'redis',
  jobs: jobRegistry,
  connections: <String, QueueConfig>{
    'database': QueueConfig(driver: 'database'),
  },
);
```

Pick a connection when you dispatch:

```dart
await SendReport(reportId: 1).dispatch(connection: 'database');
```

With no `connection`, the job goes to the main connection.
An unknown name throws an `ArgumentError`.

Run a worker for each connection. Every command of `bin/queue.dart` takes `--connection`:

```bash
dart run bin/queue.dart work
dart run bin/queue.dart work --connection=database
dart run bin/queue.dart failed --connection=database
```

- An entry uses only `driver`, `redis`, `prefix`, `connection` (the dox_orm one), `retryAfter` and `serializeSync`. The `jobs` of the main config count for all connections.
- The in-process worker (`workInProcess`) runs the jobs of the main connection only.
- Two `database` connections on the same dox_orm connection share the `jobs` and `failed_jobs` tables. Give each one its own dox_orm connection. If they share, `failed --connection=X` lists all failed jobs of the table, not only those of X.
- `Queue.fake()` keeps the name in `PushedJob.connection`.

## Small apps: a worker in the web server

You can skip the worker process. Set `workInProcess: true`:

```dart
QueueConfig queueConfig = QueueConfig(
  driver: 'database',
  jobs: jobRegistry,
  workInProcess: true,
);
```

The first isolate of the web server then runs jobs too. When the server stops, it waits for the running jobs.

> **Heavy jobs slow down your API**
> The jobs share the isolate with web requests.
> A long or CPU heavy job makes requests on that isolate slow.
> Use a worker process when your jobs are heavy or many.

## Testing

Use the `sync` driver. `dispatch()` then runs the job at once, so you can check the result right after.

```dart
import 'package:dox_queue/dox_queue.dart';
import 'package:my_app/generated/jobs.g.dart';
import 'package:test/test.dart';

void main() {
  setUp(() {
    Queue.configure(QueueConfig(driver: 'sync', jobs: jobRegistry));
  });

  test('sends the welcome email', () async {
    await SendWelcomeEmail(userId: 1).dispatch();
    // check the email was sent
  });
}
```

To check that your code dispatched a job, without running the job, use `Queue.fake()`. It keeps every dispatched job in a list. It does not run or save them. It needs no `Queue.configure()`.

```dart
import 'package:dox_queue/dox_queue.dart';
import 'package:test/test.dart';

void main() {
  late QueueFake queue;

  setUp(() {
    queue = Queue.fake();
  });

  tearDown(Queue.restore);

  test('signing up queues the welcome email', () async {
    await signUp(userId: 1);

    queue.assertPushed<SendWelcomeEmail>();
    queue.assertPushed<SendWelcomeEmail>(
      (SendWelcomeEmail job) => job.userId == 1,
    );
    queue.assertPushedTimes<SendWelcomeEmail>(1);
    queue.assertNotPushed<SendInvoice>();
  });
}
```

| Method | Checks |
|---|---|
| `assertPushed<T>([test])` | a `T` job was pushed. With `test`, one of them passes it. |
| `assertPushedTimes<T>(n)` | exactly `n` `T` jobs were pushed |
| `assertNotPushed<T>()` | no `T` job was pushed |
| `assertNothingPushed()` | no job was pushed at all |
| `assertChained([A, B])` | a chain of exactly these job types, in this order, was dispatched |
| `assertBatched([test])` | a batch was dispatched. With `test`, one of them passes it. |
| `assertBatchCount(n)` | exactly `n` batches were dispatched |

A failed check throws an `AssertionError` that says what was pushed.

To read the jobs, use `queue.pushed`. Each item has the `job`, the `queue` name and the `delay` (null when there was none). `queue.jobsOf<T>()` gives the `T` jobs. `queue.clear()` forgets them.

- `dispatchSync()` and `dispatchAfterResponse()` are not faked. They still run the job. The fake keeps their jobs in `queue.dispatchedSync` and `queue.dispatchedAfterResponse` (not in `queue.pushed`), so a test can check them.
- `Mail.queue()` and `Mail.later()` of dox_mailer go through `dispatch()`, so the fake catches them too.
- `Queue.restore()` stops the fake.

To test a job alone, call `handle()`:

```dart
await SendWelcomeEmail(userId: 1).handle();
```

To test the queue itself, use the `database` driver with SQLite `:memory:`, and run a worker that stops when the queue is empty:

```dart
await Worker(
  driver: Queue.driver,
  jobs: jobRegistry,
  options: WorkerOptions(stopWhenEmpty: true),
).run();
```

## Development

The tests use SQLite always. They also use Postgres, MariaDB and Redis when their ports are set:

```bash
PG_PORT=5432 MYSQL_PORT=3306 REDIS_PORT=6379 dart test
```

See `.github/workflows/ci.yaml` for the container settings.

For local work against the Dox repo next to this one, add a `pubspec_overrides.yaml` (it is gitignored):

```yaml
dependency_overrides:
  dox_core:
    path: ../dox/packages/dox-core
```

## Release

Set `version` in `pubspec.yaml`, add it to `CHANGELOG.md`, merge to `main`, then push a `v<version>` tag.
The Release workflow publishes the package to pub.dev.

## License

MIT. See [LICENSE](LICENSE).
