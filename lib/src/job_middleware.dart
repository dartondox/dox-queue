import 'dart:async';

import 'package:dox_core/dox_core.dart' show Cache, CacheLock;

import 'job.dart';
import 'job_run.dart';
import 'queue_driver.dart';

/// Runs the rest of the middleware, then `handle()` of the job.
typedef JobNext = Future<void> Function();

/// Wraps `handle()` of a job. A job gives
/// its list with the [HasMiddleware] mixin. The first one runs first.
///
/// A middleware calls [next] to go on. It can skip the job (it does not
/// call [next]: the job is then done and deleted), or put it back on the
/// queue with [release].
/// ```
/// class LogTime extends JobMiddleware {
///   @override
///   Future<void> handle(DoxJob job, JobNext next) async {
///     Stopwatch watch = Stopwatch()..start();
///     await next();
///     print('${job.runtimeType} took ${watch.elapsed}');
///   }
/// }
/// ```
abstract class JobMiddleware {
  const JobMiddleware();

  Future<void> handle(DoxJob job, JobNext next);

  /// Put [job] back on its queue, to run again after [delay]. It works
  /// like `release()` of `InteractsWithQueue`: the worker does it when
  /// the run ends, and the next run counts as a new try. Set `tries`
  /// above 1 (or `retryUntil`) on a job that can be released. Outside a
  /// worker (`dispatchSync()`, the `sync` driver) there is no queue: the
  /// job does not run and a warning is logged.
  void release(DoxJob job, [Duration delay = Duration.zero]) {
    JobRun? run = JobRun.of(job);
    if (run == null) return;
    run.action = JobAction.release;
    run.delay = delay;
    run.releasedBy = '$runtimeType';
  }
}

/// Lets a job run at most [max] times [per] time window. Over the limit, the
/// job goes back on the queue until the window ends. Every job with the same
/// [key] shares the limit.
///
/// The count is kept in the default cache store of the app (`Cache()`),
/// so every worker that uses the same store shares it. Use a `redis` (or
/// `file`, on one machine) store with more than one worker process.
/// ```
/// @override
/// List<JobMiddleware> get middleware => <JobMiddleware>[
///   RateLimited('mail-api', 10, Duration(minutes: 1)),
/// ];
/// ```
class RateLimited extends JobMiddleware {
  const RateLimited(this.key, this.max, this.per);

  final String key;
  final int max;
  final Duration per;

  @override
  Future<void> handle(DoxJob job, JobNext next) async {
    String name = 'dox_queue:limit:$key';
    int hits = await _Limiter.hit(name, per);
    if (hits > max) {
      release(job, await _Limiter.availableIn(name));
      return;
    }
    await next();
  }
}

/// Lets only one job with the same [key] run at a time. When another one runs,
/// the job goes back on the queue after [releaseAfter]. Null [releaseAfter]:
/// the job is dropped (deleted) instead.
///
/// [expireAfter] frees the lock after this time, in case a worker
/// crashed while it held it. Null: the lock is only freed when the job
/// ends. It uses a lock of the default cache store, see [RateLimited].
///
/// The class name is not part of the lock: all job
/// classes with the same [key] share it. Put the class name in [key] when
/// they should not.
class WithoutOverlapping extends JobMiddleware {
  const WithoutOverlapping(
    this.key, {
    this.releaseAfter = Duration.zero,
    this.expireAfter,
  });

  final String key;
  final Duration? releaseAfter;
  final Duration? expireAfter;

  @override
  Future<void> handle(DoxJob job, JobNext next) async {
    CacheLock lock = Cache().lock('dox_queue:overlap:$key', expireAfter);
    if (await lock.get()) {
      try {
        await next();
      } finally {
        await lock.release();
      }
      return;
    }
    Duration? wait = releaseAfter;
    if (wait != null) release(job, wait);
  }
}

/// After [maxAttempts] errors of jobs with the same [key] in [decay] time, stop
/// running them until [decay] is over. Use it for a service that is down: the
/// jobs wait instead of failing at once.
///
/// When `handle()` throws, the job goes back on the queue after
/// [retryAfter] and the error is logged. Over the limit, the job goes
/// back until the window ends. Each release counts as a try, so give the
/// job a `retryUntil`. A run with no error resets the count. It uses the
/// default cache store, see [RateLimited].
class ThrottlesExceptions extends JobMiddleware {
  const ThrottlesExceptions(
    this.key, {
    this.maxAttempts = 10,
    this.decay = const Duration(minutes: 10),
    this.retryAfter = Duration.zero,
  });

  final String key;
  final int maxAttempts;
  final Duration decay;
  final Duration retryAfter;

  @override
  Future<void> handle(DoxJob job, JobNext next) async {
    String name = 'dox_queue:throttle:$key';
    num hits = await Cache().get(name, 0) as num;
    if (hits >= maxAttempts) {
      release(job, await _Limiter.availableIn(name));
      return;
    }
    try {
      await next();
    } catch (_) {
      await _Limiter.hit(name, decay);
      // a release wins over the error: the worker logs the error and
      // puts the job back. Outside a worker the error goes on. A fail()
      // or delete() of the job itself is kept.
      JobAction? chosen = JobRun.of(job)?.action;
      if (chosen != JobAction.fail && chosen != JobAction.delete) {
        release(job, retryAfter);
      }
      rethrow;
    }
    if (hits > 0) {
      await Cache().forget(name);
      await Cache().forget('$name:timer');
    }
  }
}

/// A hit counter in the cache. The count
/// lives for one window. `<name>:timer` keeps the time the window ends.
class _Limiter {
  /// count one hit and return the count of this window
  static Future<int> hit(String name, Duration window) async {
    Cache cache = Cache();
    int ends = QueueDriver.now() + window.inMilliseconds;
    await cache.add('$name:timer', ends, duration: window);
    bool added = await cache.add(name, 0, duration: window);
    int hits = await cache.increment(name);
    // the key ran out between add and increment: it has no time limit
    // now, so give it one
    if (!added && hits == 1) await cache.put(name, 1, duration: window);
    return hits;
  }

  /// the time until the window of [name] ends
  static Future<Duration> availableIn(String name) async {
    num? ends = await Cache().get('$name:timer') as num?;
    int wait = (ends ?? 0).toInt() - QueueDriver.now();
    return Duration(milliseconds: wait < 0 ? 0 : wait);
  }
}
