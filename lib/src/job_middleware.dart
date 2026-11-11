import 'dart:async';

import 'job.dart';
import 'job_run.dart';

/// Runs the rest of the middleware, then `handle()` of the job.
typedef JobNext = Future<void> Function();

/// Wraps `handle()` of a job, like Laravel job middleware. A job gives
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
  /// worker it does nothing.
  void release(DoxJob job, [Duration delay = Duration.zero]) {
    JobRun? run = JobRun.of(job);
    if (run == null) return;
    run.action = JobAction.release;
    run.delay = delay;
  }
}
