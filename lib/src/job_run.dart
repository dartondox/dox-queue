import 'job.dart';
import 'job_middleware.dart';

/// What a job asked for inside `handle()`
enum JobAction { release, fail, delete }

/// One run of a job: its try number, and what the job asked for with
/// `release()`, `fail()` or `delete()`. Not exported: the worker and
/// the queue use it.
class JobRun {
  JobRun(this.attempts);

  /// see `DoxJob.attempts()`
  final int attempts;

  /// null: the job asked for nothing. The last call wins.
  JobAction? action;

  /// the wait of `release()`
  Duration delay = Duration.zero;

  /// the error of `fail()`
  Object? error;
  StackTrace? stack;

  /// true when `handle()` returned or threw. Still false when the
  /// worker timeout fired first.
  bool ended = false;

  /// The run of each running job. An [Expando], so a job class needs no
  /// field and its constructor can stay `const`. Per isolate, like the
  /// job object itself.
  static final Expando<JobRun> _runs = Expando<JobRun>('dox_queue job run');

  /// the run of [job], or null when it does not run now
  static JobRun? of(DoxJob job) => _runs[job];

  /// Run `job.handle()` with this run set, so `attempts()`, `release()`,
  /// `fail()` and `delete()` work. The run ends when [handle] ends or
  /// throws. A late call (eg. after a timeout) then does nothing.
  Future<void> handle(DoxJob job) async {
    _runs[job] = this;
    try {
      if (job is HasMiddleware) {
        await _through(job, job.middleware, 0);
      } else {
        await job.handle();
      }
    } finally {
      ended = true;
      if (identical(_runs[job], this)) _runs[job] = null;
    }
  }

  /// middleware number [index] and the ones after it, then `handle()`
  static Future<void> _through(
    DoxJob job,
    List<JobMiddleware> middleware,
    int index,
  ) {
    if (index == middleware.length) return job.handle();
    return middleware[index].handle(job, () {
      return _through(job, middleware, index + 1);
    });
  }
}
