# Job mixins, middleware, unique and encrypted jobs

Control the job from inside `handle()` with the `InteractsWithQueue` mixin:

```dart
@Job()
class SendReport extends DoxJob with InteractsWithQueue {
  SendReport({required this.reportId});

  final int reportId;

  @override
  int get tries => 5;

  @override
  Future<void> handle() async {
    if (attempts() > 3) {
      fail(StateError('too many tries')); // fail now, no retry
      return;
    }
    release(const Duration(seconds: 30)); // run again later (counts as a try)
  }
}
```

Other calls: `delete()` (drop the job, no `failed()`). The last call wins. A `release()` counts as a try, so set `tries` above 1.

## Job middleware

```dart
@Job()
class SendReport extends DoxJob with HasMiddleware {
  @override
  int get tries => 5;

  @override
  List<JobMiddleware> get middleware => <JobMiddleware>[
    RateLimited('report-api', 10, const Duration(minutes: 1)),
    WithoutOverlapping('report-$reportId'),
  ];
  // ...
}
```

Built in: `RateLimited(key, max, per)`, `WithoutOverlapping(key, releaseAfter:, expireAfter:)`, `ThrottlesExceptions(key, maxAttempts:, decay:, retryAfter:)`.
They keep counts in the default cache store. Workers must share one store (`redis`, or `file` on one machine).
A custom one extends `JobMiddleware` and overrides `Future<void> handle(DoxJob job, JobNext next)`. Call `await next()` to go on.

## Unique and encrypted jobs

```dart
@Job()
class UpdateSearchIndex extends DoxJob with ShouldBeUnique {
  UpdateSearchIndex({required this.productId});

  final int productId;

  @override
  String get uniqueId => '$productId';

  @override
  Duration? get uniqueFor => const Duration(hours: 1);
  // ...
}

@Job()
class ChargeCard extends DoxJob with ShouldBeEncrypted {
  // data is saved encrypted with AppConfig.appKey
}
```

- While a unique job with the same name and `uniqueId` is waiting or running, `dispatch()` pushes nothing and returns `''`.
- Always set `uniqueFor`. A crashed worker leaves the lock, and with no `uniqueFor` it never ends.
- `ShouldBeUniqueUntilProcessing` frees the lock when the job starts.
- The lock is in the default cache store. The web app and the workers must share it.
- Encrypted jobs need the same app key (or one in `AppConfig.previousKeys`) in the worker.

