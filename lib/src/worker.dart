import 'dart:async';
import 'dart:convert';
import 'dart:io' show ProcessInfo;

import 'job.dart';
import 'job_registry.dart';
import 'job_run.dart';
import 'queue.dart';
import 'queue_driver.dart';
import 'queue_event.dart';
import 'queue_log.dart';
import 'unique_lock.dart';

/// Settings of a [Worker]. The flags of `bin/queue.dart work` set them.
class WorkerOptions {
  const WorkerOptions({
    this.queues = const <String>['default'],
    this.concurrency = 1,
    this.once = false,
    this.tries = 1,
    this.timeout = const Duration(seconds: 60),
    this.sleep = const Duration(seconds: 3),
    this.stopWhenEmpty = false,
    this.timeoutGrace,
    this.backoff = const <Duration>[],
    this.maxJobs,
    this.maxTime,
    this.memory,
  });

  /// the queues to work, first one first (`--queue=high,default`)
  final List<String> queues;

  /// jobs at once in this process (`--concurrency=4`). They run as
  /// async tasks on one isolate, so they help for jobs that wait on
  /// I/O (mail, http, database), not for CPU heavy jobs.
  final int concurrency;

  /// run one job, then stop (`--once`)
  final bool once;

  /// tries of a job that does not set `tries` (`--tries=3`)
  final int tries;

  /// max run time of a job that does not set `timeout` (`--timeout=60`)
  final Duration timeout;

  /// the wait when the queues are empty (`--sleep=3`)
  final Duration sleep;

  /// stop when the queues are empty (`--stop-when-empty`)
  final bool stopWhenEmpty;

  /// Opt-in (`--timeout-grace=SECONDS`). Dart can not stop a job that
  /// timed out, so its `handle()` keeps running. With this set, the
  /// worker waits for that run to end, up to this time, before it
  /// retries or fails the job. So the retry does not run at the same
  /// time as the old run. The worker takes no other job while it waits.
  /// Keep `timeout + timeoutGrace` below the driver `retryAfter`, or
  /// another worker can take the job again while this one waits.
  /// Null (the default): no wait, the retry can start at once.
  final Duration? timeoutGrace;

  /// the wait before each retry of a job with no `backoff` of its own
  /// (`--backoff=10` or `--backoff=1,5,10`, in seconds). The last entry
  /// is used for the rest. Empty (the default): retry at once.
  final List<Duration> backoff;

  /// stop after this many jobs (`--max-jobs=1000`). Null: no limit.
  final int? maxJobs;

  /// stop after this time (`--max-time=3600`). The running jobs end
  /// first. Null: no limit.
  final Duration? maxTime;

  /// stop after a job when the process uses more than this many MB of
  /// memory (`--memory=256`). Null: no limit.
  final int? memory;
}

/// Takes jobs from the queue and runs them. A job that throws is tried
/// again after its backoff, up to its tries. Then it goes to the failed
/// jobs and its `failed()` hook runs.
class Worker {
  Worker({
    required this.driver,
    required this.jobs,
    this.options = const WorkerOptions(),
    this.popStopGrace = defaultPopStopGrace,
  });

  /// How long a pop that is already running may take after [stop]. If it
  /// returns a job in this time, the job runs. After it, [run] stops
  /// waiting for the pop, so a stuck pop (eg. Redis is down) can not hold
  /// the worker.
  static const Duration defaultPopStopGrace = Duration(seconds: 5);

  final QueueDriver driver;
  final JobRegistry jobs;
  final WorkerOptions options;

  /// the grace of [defaultPopStopGrace], set here to make tests fast
  final Duration popStopGrace;

  bool _stopping = false;

  /// true when [run] is over, so a late [stop] starts no timer
  bool _runEnded = false;

  /// the pops that run now, one completer for each
  final Set<Completer<ReservedJob?>> _waiting = <Completer<ReservedJob?>>{};

  /// ends the wait for the running pops, started by the first [stop]
  Timer? _graceTimer;

  /// completes on [stop], to end a sleep early
  final Completer<void> _wake = Completer<void>();

  /// how many jobs this worker ran, for [WorkerOptions.maxJobs]
  int _done = 0;

  /// Work until [stop], or until the queues are empty with
  /// `stopWhenEmpty`, or after one job with `once`, or until a limit of
  /// `maxJobs`, `maxTime` or `memory` is hit.
  Future<void> run() async {
    int loops = options.once ? 1 : options.concurrency;
    Duration? maxTime = options.maxTime;
    Timer? timer = maxTime == null ? null : Timer(maxTime, _timeIsUp);
    try {
      await Future.wait(<Future<void>>[
        for (int i = 0; i < loops; i++) _loop(),
      ]);
    } finally {
      _runEnded = true;
      timer?.cancel();
      _graceTimer?.cancel();
    }
    await const QueueEvent(QueueEventType.stopping).fire();
  }

  void _timeIsUp() {
    QueueLog.info('Queue: max time ${options.maxTime} reached, stopping');
    stop();
  }

  /// Stop taking jobs. [run] ends when the running jobs are done.
  void stop() {
    _stopping = true;
    if (!_wake.isCompleted) _wake.complete();
    if (!_runEnded) _graceTimer ??= Timer(popStopGrace, _giveUpWaiting);
  }

  /// A job reserved after this comes back after the retryAfter time of
  /// the driver, with one more attempt.
  void _giveUpWaiting() {
    for (Completer<ReservedJob?> waiting in _waiting.toList()) {
      if (!waiting.isCompleted) waiting.complete(null);
    }
    _waiting.clear();
  }

  Future<void> _loop() async {
    while (!_stopping) {
      await const QueueEvent(QueueEventType.looping).fire();
      if (_stopping) return;
      ReservedJob? job;
      try {
        job = await _nextOrStop();
      } catch (error, stack) {
        QueueLog.danger('Queue: can not take a job: $error', '$stack');
        await _pause(options.sleep);
        continue;
      }
      if (job == null) {
        if (_stopping) return;
        if (options.stopWhenEmpty || options.once) return;
        await _pause(options.sleep);
        continue;
      }
      try {
        await process(job);
      } catch (error, stack) {
        QueueLog.danger('Queue: ${job.payload.job} error: $error', '$stack');
      }
      if (options.once) return;
      _checkLimits();
    }
  }

  /// stop on [WorkerOptions.maxJobs] or [WorkerOptions.memory]
  void _checkLimits() {
    _done++;
    int? maxJobs = options.maxJobs;
    if (maxJobs != null && _done >= maxJobs && !_stopping) {
      QueueLog.info('Queue: ran $_done jobs (max $maxJobs), stopping');
      stop();
    }
    int? memory = options.memory;
    if (memory != null && ProcessInfo.currentRss > memory * 1024 * 1024) {
      QueueLog.info('Queue: memory is over $memory MB, stopping');
      stop();
    }
  }

  /// Like [_next], but gives null when the grace after [stop] is over.
  /// A pop can hang for ever (eg. Redis is down), so the loop must not
  /// wait for it. Each pop has its own completer, so no listener stays on
  /// a shared long-lived future.
  Future<ReservedJob?> _nextOrStop() {
    Completer<ReservedJob?> waiting = Completer<ReservedJob?>();
    _waiting.add(waiting);
    _popInto(waiting);
    return waiting.future;
  }

  Future<void> _popInto(Completer<ReservedJob?> waiting) async {
    try {
      ReservedJob? job = await _next();
      if (!waiting.isCompleted) {
        waiting.complete(job);
      } else if (job != null) {
        await _giveBack(job);
      }
    } catch (error, stack) {
      if (!waiting.isCompleted) waiting.completeError(error, stack);
    } finally {
      _waiting.remove(waiting);
    }
  }

  /// The pop was given up after the grace, and then it returned a job.
  /// Nobody runs it, so put it back on the queue at once. The pop already
  /// counted one attempt, and release keeps it (the same as a normal
  /// release), so the job has one less try. This is the best we can do.
  /// The driver may be closed by now, so errors are ignored.
  Future<void> _giveBack(ReservedJob job) async {
    try {
      await driver.release(job, Duration.zero);
    } catch (_) {
      // the reservation of the driver ends by itself (retryAfter)
    }
  }

  Future<ReservedJob?> _next() async {
    for (String queue in options.queues) {
      if (_stopping) return null;
      ReservedJob? job = await driver.pop(queue);
      if (job != null) return job;
    }
    return null;
  }

  /// sleep, but wake up on [stop]
  Future<void> _pause(Duration time) async {
    Completer<void> done = Completer<void>();
    Timer timer = Timer(time, done.complete);
    await Future.any(<Future<void>>[done.future, _wake.future]);
    timer.cancel();
  }

  /// Run one reserved job, then delete, release or fail it.
  Future<void> process(ReservedJob reserved) async {
    JobPayload payload = reserved.payload;
    String label = '${payload.job} ${payload.id}';
    DoxJob job;
    try {
      job = _decode(payload);
    } catch (error, stack) {
      // no retry: the job can not be read, a retry gives the same error
      QueueLog.danger('Queue: failed $label: $error');
      await driver.fail(reserved, '$error\n$stack');
      await _freeUnique(payload);
      await JobBus.failed(payload, jobs, error);
      await _event(
        QueueEventType.failed,
        reserved,
        null,
        error: error,
        stack: stack,
      );
      return;
    }
    JobBus.read(job, payload);
    await _event(QueueEventType.processing, reserved, job);
    if (job is ShouldBeUniqueUntilProcessing) await _freeUnique(payload);
    if (reserved.attempts > 1 &&
        _noTryLeft(job, payload, reserved.attempts - 1)) {
      // a worker took it before and did not finish (it crashed or the
      // reservation timed out), or its retryUntil time passed
      StateError error = StateError(
        payload.retryUntil != null
            ? '$label is past its retryUntil time.'
            : '$label was tried ${reserved.attempts - 1} times and did not '
                  'finish. The worker may have crashed, or the job ran '
                  'longer than retryAfter.',
      );
      await _fail(reserved, job, label, error, StackTrace.current);
      return;
    }
    Duration timeout = job.timeout ?? options.timeout;
    Stopwatch watch = Stopwatch()..start();
    QueueLog.info('Queue: running $label');
    Future<void>? run;
    JobRun jobRun = JobRun(reserved.attempts);
    try {
      run = jobRun.handle(job);
      await run.timeout(timeout);
    } catch (error, stack) {
      bool timedOut = error is TimeoutException;
      // handle() ended with an error after release, fail or delete: the
      // call wins, also over its own TimeoutException. When the worker
      // timeout fired, the call is dropped: the timeout rules count.
      if (jobRun.ended && jobRun.action != null) {
        QueueLog.warn(
          'Queue: $label threw after ${jobRun.action!.name}(): $error',
        );
        await _apply(jobRun, reserved, job, label);
        return;
      }
      Object reason = error;
      if (timedOut) {
        reason = TimeoutException('$label timed out after $timeout', timeout);
        if (run != null) await _waitForTimedOutRun(run, label);
      }
      await _tryFailed(reserved, job, label, reason, stack, timedOut: timedOut);
      return;
    }
    if (jobRun.action != null) {
      await _apply(jobRun, reserved, job, label);
      return;
    }
    if (await _finish(reserved, job, label)) {
      QueueLog.success('Queue: done $label in ${watch.elapsedMilliseconds}ms');
    }
  }

  /// A try of the job failed with [reason]: release it for the next
  /// try, or fail it when no try is left. A timeout does not count as
  /// an exception.
  Future<void> _tryFailed(
    ReservedJob reserved,
    DoxJob job,
    String label,
    Object reason,
    StackTrace stack, {
    bool timedOut = false,
  }) async {
    JobPayload payload = reserved.payload;
    int exceptions = timedOut ? payload.exceptions : payload.exceptions + 1;
    int tries = job.tries ?? options.tries;
    ReservedJob next = ReservedJob(
      payload.withExceptions(exceptions),
      attempts: reserved.attempts,
      ref: reserved.ref,
    );
    int? maxExceptions = job.maxExceptions;
    if ((timedOut && job.failOnTimeout) ||
        (maxExceptions != null && exceptions >= maxExceptions) ||
        _noTryLeft(job, payload, reserved.attempts)) {
      await _fail(next, job, label, reason, stack);
      return;
    }
    Duration wait = backoffFor(job, reserved.attempts, options.backoff);
    String of = payload.retryUntil == null ? ' of $tries' : '';
    QueueLog.warn(
      'Queue: $label failed try ${reserved.attempts}$of, '
      'next try in $wait: $reason',
    );
    await _event(
      QueueEventType.exceptionOccurred,
      next,
      job,
      error: reason,
      stack: stack,
    );
    await driver.release(next, wait);
    await _event(QueueEventType.released, next, job, delay: wait);
  }

  /// The job ended: push the next job of its chain, delete it, free its
  /// unique lock and count it in its batch. False when the push of the
  /// next job threw: then the try counts as failed, so the push is tried
  /// again with the job (the job runs again).
  Future<bool> _finish(ReservedJob reserved, DoxJob job, String label) async {
    JobPayload payload = reserved.payload;
    if (payload.chain?.isNotEmpty ?? false) {
      // free the lock first, so a next job of the same unique id is not
      // skipped because of this job. A retry then runs without the lock.
      await _releaseUnique(reserved, job);
      try {
        await JobBus.next(payload, driver, jobs);
      } catch (error, stack) {
        await _tryFailed(
          reserved,
          job,
          label,
          StateError('can not push the next job of the chain: $error'),
          stack,
        );
        return false;
      }
      await driver.delete(reserved);
    } else {
      await driver.delete(reserved);
      await _releaseUnique(reserved, job);
    }
    await JobBus.done(payload, jobs);
    await _event(QueueEventType.processed, reserved, job);
    return true;
  }

  /// fire a queue event of [reserved]
  Future<void> _event(
    QueueEventType type,
    ReservedJob reserved,
    DoxJob? job, {
    Object? error,
    StackTrace? stack,
    Duration? delay,
  }) {
    JobPayload payload = reserved.payload;
    return QueueEvent(
      type,
      job: job,
      name: payload.job,
      id: payload.id,
      queue: payload.queue,
      attempts: reserved.attempts,
      error: error,
      stack: stack,
      delay: delay,
    ).fire();
  }

  /// free the lock of a [ShouldBeUnique] job, unless it was freed at the
  /// start ([ShouldBeUniqueUntilProcessing])
  Future<void> _releaseUnique(ReservedJob reserved, DoxJob job) async {
    if (job is ShouldBeUniqueUntilProcessing) return;
    await _freeUnique(reserved.payload);
  }

  /// free the unique lock saved in [payload], if it has one
  static Future<void> _freeUnique(JobPayload payload) async {
    String? name = payload.unique;
    if (name != null) await releaseUniqueLock(name, payload.id);
  }

  /// do what the job asked for with `release()`, `fail()` or `delete()`
  Future<void> _apply(
    JobRun jobRun,
    ReservedJob reserved,
    DoxJob job,
    String label,
  ) async {
    switch (jobRun.action!) {
      case JobAction.release:
        QueueLog.info('Queue: $label released, next try in ${jobRun.delay}');
        await driver.release(reserved, jobRun.delay);
        await _event(
          QueueEventType.released,
          reserved,
          job,
          delay: jobRun.delay,
        );
      case JobAction.delete:
        if (await _finish(reserved, job, label)) {
          QueueLog.info('Queue: $label deleted by the job');
        }
      case JobAction.fail:
        await _fail(reserved, job, label, jobRun.error!, jobRun.stack!);
    }
  }

  /// With [WorkerOptions.timeoutGrace], wait until the timed-out [run]
  /// ends, or the grace time is over. Its late result is ignored: the
  /// try already counts as failed.
  Future<void> _waitForTimedOutRun(Future<void> run, String label) async {
    Duration? grace = options.timeoutGrace;
    if (grace == null) return;
    bool ended = await run
        .then(_ended, onError: _endedWithError)
        .timeout(grace, onTimeout: _stillRunning);
    if (!ended) {
      QueueLog.warn(
        'Queue: $label still runs $grace after its timeout. '
        'The next try may run at the same time.',
      );
    }
  }

  static bool _ended(void _) => true;
  static bool _endedWithError(Object _) => true;
  static bool _stillRunning() => false;

  /// No try is left after try number [attempts]: the time of
  /// `retryUntil` has passed, or else all `tries` are used.
  bool _noTryLeft(DoxJob job, JobPayload payload, int attempts) {
    int? until = payload.retryUntil;
    if (until != null) return QueueDriver.now() >= until;
    return attempts >= (job.tries ?? options.tries);
  }

  /// the wait before the next try, after try number [attempts]. A job
  /// with no `backoff` uses [fallback] (the worker `--backoff`).
  static Duration backoffFor(
    DoxJob job,
    int attempts, [
    List<Duration> fallback = const <Duration>[],
  ]) {
    List<Duration> backoff = job.backoff.isEmpty ? fallback : job.backoff;
    if (backoff.isEmpty) return Duration.zero;
    int index = attempts - 1;
    return backoff[index < backoff.length ? index : backoff.length - 1];
  }

  DoxJob _decode(JobPayload payload) {
    JobCodec? codec = jobs.byName(payload.job);
    if (codec == null) {
      throw StateError(
        'No job class named ${payload.job} in the job registry. Run '
        '`dart run build_runner build`, and restart the worker.',
      );
    }
    String data = Queue.readData(payload.data, payload.job);
    return codec.decode(jsonDecode(data) as Map<String, Object?>);
  }

  Future<void> _fail(
    ReservedJob reserved,
    DoxJob job,
    String label,
    Object error,
    StackTrace stack,
  ) async {
    QueueLog.danger('Queue: failed $label: $error');
    await driver.fail(reserved, '$error\n$stack');
    await _releaseUnique(reserved, job);
    try {
      await job.failed(error, stack);
    } catch (hookError, hookStack) {
      QueueLog.danger(
        'Queue: failed() of $label threw: $hookError',
        '$hookStack',
      );
    }
    await JobBus.failed(reserved.payload, jobs, error);
    await _event(
      QueueEventType.failed,
      reserved,
      job,
      error: error,
      stack: stack,
    );
  }
}
