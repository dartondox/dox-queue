---
name: dox-queue-guide
description: Use when you add a queue job, dispatch or delay a job, run the queue worker, set up the redis or database queue driver (dox_queue_database), handle failed jobs, chain or batch jobs, make a job unique or encrypted, schedule a job, or test dispatches with Queue.fake() in a Dox (Dart) app.
---

# dox_queue guide

`dox_queue` runs slow work in the background. The request answers at once. A worker process runs the job later.
It has two drivers (`sync`, `redis`), retries, delays, failed jobs, chains and batches. The `database` driver, batches and `afterCommit` need the `dox_queue_database` package.
For routes, controllers and services, see the dox-core-guide skill.
Detail on chains, batches and driver setup is in `references/`.

## Install and set up

```bash
dart pub add dox_queue
```

Your app already runs `build_runner`. `dox_queue` adds a builder that writes `lib/generated/jobs.g.dart`.

1. Config, `lib/config/queue.dart`:

```dart
import 'package:dox_queue/dox_queue.dart';

import '../generated/jobs.g.dart';

QueueConfig queueConfig = QueueConfig(
  driver: 'redis', // sync, redis or database (dox_queue_database)
  jobs: jobRegistry,  // every @Job class
);
```

2. Provider, `lib/providers/queue_provider.dart`. The generated container finds it.

```dart
import 'package:dox_queue/dox_queue.dart';

import '../config/queue.dart';

class AppQueueProvider extends QueueProvider {
  AppQueueProvider() : super(queueConfig);
}
```

3. Worker file, `bin/queue.dart`:

```dart
import 'dart:io';

import 'package:dox_queue/dox_queue.dart';
import 'package:my_app/config/app.dart';

Future<void> main(List<String> args) async {
  exitCode = await QueueWorker(app: appConfig).run(args);
}
```

The worker starts your providers (database, cache, queue). It does not start the HTTP server.

4. Database driver, batches and `afterCommit` (optional). Install `dox_queue_database`, call `QueueDatabase.register()` in the provider before `super.setup()`, then make the tables:

```bash
dart pub add dox_queue_database
dox queue:table   # writes a migration for jobs, failed_jobs and job_batches
dox migrate
```

Provider with it:

```dart
import 'package:dox_queue/dox_queue.dart';
import 'package:dox_queue_database/dox_queue_database.dart';

import '../config/queue.dart';

class AppQueueProvider extends QueueProvider {
  AppQueueProvider() : super(queueConfig);

  @override
  Future<void> setup() async {
    QueueDatabase.register();
    await super.setup();
  }
}
```

`queue:table` does not overwrite an existing `create_queue_tables` migration.

Main `QueueConfig` options: `driver` (`sync` default, `redis`, `database`), `jobs`, `redis`, `connection` (dox_orm name), `retryAfter` (90 seconds), `workInProcess`, `afterCommit`, `connections`. All options and the driver details are in `references/config-and-drivers.md`.

Keep `retryAfter` bigger than your longest job. A longer job looks crashed, so a second worker runs it again.

## Recipes

### 1. Write a job

`lib/app/jobs/send_welcome_email.job.dart`:

```dart
import 'package:dox_core/dox_core.dart';
import 'package:dox_queue/dox_queue.dart';

@Job()
class SendWelcomeEmail extends DoxJob {
  SendWelcomeEmail({required this.userId});

  final int userId;

  @override
  int get tries => 3;

  @override
  List<Duration> get backoff => const <Duration>[
    Duration(seconds: 10),
    Duration(seconds: 60),
  ];

  @override
  Duration? get timeout => const Duration(seconds: 30);

  @override
  String get queue => 'emails';

  @override
  Future<void> handle() async {
    MailService mail = Dox().ioc.get<MailService>();
    await mail.sendWelcome(userId);
  }

  /// runs once, after the last try failed
  @override
  Future<void> failed(Object error, StackTrace stack) async {}
}
```

Then run `dart run build_runner build`. Do this after every job change.

All settings are optional: `tries` (worker `--tries`, default 1), `backoff` (none), `timeout` (worker `--timeout`, 60 seconds), `retryUntil`, `maxExceptions`, `failOnTimeout`, `queue` (`default`), `failed()`.

Saved data: the parameters of the unnamed constructor. Each needs a field or getter of the same name.
Allowed types: `int`, `double`, `num`, `String`, `bool`, `DateTime`, enums, `List` and `Map<String, ...>` of these, `dynamic` and `Object` (JSON only), and nullable versions.
Any other type stops the build. Save an id (`userId`), not an object. Load the object in `handle()`.

A job has no constructor injection. Get services in `handle()` with `Dox().ioc.get<T>()`.
To rename a job class and keep queued jobs, use `@Job(name: 'OldName')`.

### 2. Dispatch

```dart
await SendWelcomeEmail(userId: user.id).dispatch(); // returns the job id
await Queue.dispatch(SendWelcomeEmail(userId: 1));  // same

await SendWelcomeEmail(userId: 1).dispatch(
  delay: const Duration(minutes: 10),
  queue: 'emails',
);
await SendWelcomeEmail(userId: 1).dispatch(delayUntil: DateTime.utc(2030, 1, 1, 9));

await SendWelcomeEmail(userId: 1).dispatchIf(user.wantsEmail);     // null when skipped
await SendWelcomeEmail(userId: 1).dispatchUnless(user.isBlocked);
await SendWelcomeEmail(userId: 1).dispatchSync();                  // now, no queue
SendWelcomeEmail(userId: 1).dispatchAfterResponse();               // after the response, no retry
```

Give `delay` or `delayUntil`, not both.
`dispatchAfterResponse()` is not on the queue. It has no id and no retry. A stop of the server loses it.

Needs `dox_queue_database`. Inside a database transaction, pass `afterCommit: true` so the job is pushed only after the commit:

```dart
await QueryBuilder.transaction(() async {
  User user = await User().create(<String, dynamic>{'email': email});
  await SendWelcomeEmail(userId: user.id).dispatch(afterCommit: true);
});
```

Chains and batches take it too: `Queue.batch(jobs).dispatch(afterCommit: true)` saves and pushes the batch after the commit. A rollback drops it.

To queue an email with no job class, use `Mail.queue(message)` and `Mail.later(Duration(minutes: 10), message)` from dox_mailer. The mail jobs are in dox_mailer, not here.

### 3. Run the worker

```bash
dart run bin/queue.dart                                  # same as: work
dart run bin/queue.dart work --queue=emails,default --concurrency=4
dart run bin/queue.dart work --once
```

| Command | What it does |
| --- | --- |
| `work` | run jobs (default) |
| `failed` | list failed jobs |
| `retry <id>`, `retry all` | put failed jobs back on their queue |
| `forget <id>`, `flush` | delete one or all failed jobs |
| `clear [queue]` | delete waiting, delayed and running jobs of a queue |
| `restart` | stop running workers after their current jobs |
| `prune-failed` | delete failed jobs older than `--hours=N` (default 24) |
| `prune-batches` | delete finished batches older than `--hours=N` (default 24) |

Flags of `work`: `--queue=a,b` (first goes first), `--concurrency=N`, `--once`, `--tries=N`, `--timeout=SECONDS`, `--sleep=SECONDS`, `--stop-when-empty`, `--timeout-grace=SECONDS`, `--backoff=1,5,10`, `--max-jobs=N`, `--max-time=SECONDS`, `--memory=MB`. Every command takes `--connection=name`.

Run the worker as its own process next to the web server.
Ctrl+C or SIGTERM stops it after the running jobs. A second signal stops it at once.
Restart the worker after each deploy. It keeps your code in memory.
`restart` needs a shared cache store (`file` on one machine, or `redis`), not `memory`.

### 4. Failed jobs and retries

When `handle()` throws, the worker waits `backoff`, then tries again, up to `tries`. After the last try the job goes to the failed jobs and `failed()` runs.
A timeout counts as a failed try. Dart cannot stop a running `Future`, so `handle()` keeps running after a timeout. Write jobs that are safe to run twice, or use `--timeout-grace`.
A crashed worker leaves its job reserved. After `retryAfter` another worker takes it, and that counts as a try.

Control a running job (`attempts()`, `release(delay)`, `fail(error)`, `delete()`), add job middleware (`RateLimited`, `WithoutOverlapping`, `ThrottlesExceptions`), and make jobs unique (`ShouldBeUnique`) or encrypted (`ShouldBeEncrypted`) with opt-in mixins. See `references/job-mixins.md`.
Always set `uniqueFor` on a unique job. A crashed worker leaves the lock, and with no `uniqueFor` it never ends.
A `release()` counts as a try, so set `tries` above 1.

### 5. Chains and batches

```dart
await Queue.chain(<DoxJob>[
  ResizeImage(imageId: 1),
  UploadImage(imageId: 1),
]).catchError(TellAdmin(imageId: 1)).dispatch(queue: 'images');

Batch batch = await Queue.batch(<DoxJob>[
  ImportRows(from: 0),
  ImportRows(from: 1000),
]).name('import').then(ImportDone()).dispatch(queue: 'imports');
```

Callbacks are jobs too, from `jobRegistry`, because a function cannot be saved. Batches need `dox_queue_database` and the `job_batches` table.
See `references/chains-and-batches.md`.

### 6. Schedule a job

With dox_scheduler, a task calls `dispatch()`:

```dart
import 'package:dox_scheduler/dox_scheduler.dart';

void schedule(Schedule s) {
  s.call('send-report', () => SendDailyReport().dispatch()).dailyAt('08:00');
  s.call('prune', () => PruneFiles().dispatch(connection: 'redis')).hourly();
}
```

Task names must be unique.

### 7. Test

Use `Queue.fake()` to check that code dispatched a job. It runs and saves nothing, and needs no `Queue.configure()`.

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
    queue.assertPushed<SendWelcomeEmail>((SendWelcomeEmail job) => job.userId == 1);
    queue.assertPushedTimes<SendWelcomeEmail>(1);
    queue.assertNotPushed<SendInvoice>();
  });
}
```

Also: `assertNothingPushed()`, `assertChained([A, B])`, `assertBatched([test])`, `assertBatchCount(n)`, `queue.pushed`, `queue.jobsOf<T>()`, `queue.clear()`.
`dispatchSync()` and `dispatchAfterResponse()` still run. The fake keeps them in `queue.dispatchedSync` and `queue.dispatchedAfterResponse`.

To run jobs in a test, use the sync driver: `Queue.configure(QueueConfig(driver: 'sync', jobs: jobRegistry));`.
To test one job, call `await SendWelcomeEmail(userId: 1).handle();`.

## Gotchas

- Run `dart run build_runner build` after any job change. A job missing from `jobRegistry` fails at dispatch or in the worker.
- The default `tries` is 1. Set `tries` on jobs that retry, release, or sit in a chain. Deploy the worker with the web app, or an old worker fails new jobs.
- A web server alone runs no jobs. Start `bin/queue.dart` (or set `workInProcess: true` for small apps).
- Mixins are opt-in: `InteractsWithQueue`, `HasMiddleware`, `ShouldBeUnique`, `ShouldBeEncrypted`, `CatchesError`, `Batchable`.
- With the `sync` driver there is no queue: `delay` is ignored, `release()` does nothing, and a throwing job makes `dispatch()` throw.
