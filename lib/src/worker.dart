import 'dart:async';
import 'dart:convert';

import 'package:dox_core/utils/logger.dart';

import 'job.dart';
import 'job_registry.dart';
import 'queue_driver.dart';

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
}

/// Takes jobs from the queue and runs them. A job that throws is tried
/// again after its backoff, up to its tries. Then it goes to the failed
/// jobs and its `failed()` hook runs.
class Worker {
  Worker({
    required this.driver,
    required this.jobs,
    this.options = const WorkerOptions(),
  });

  final QueueDriver driver;
  final JobRegistry jobs;
  final WorkerOptions options;

  bool _stopping = false;

  /// completes on [stop], to end a sleep early
  final Completer<void> _wake = Completer<void>();

  /// Work until [stop], or until the queues are empty with
  /// `stopWhenEmpty`, or after one job with `once`.
  Future<void> run() async {
    int loops = options.once ? 1 : options.concurrency;
    await Future.wait(<Future<void>>[for (int i = 0; i < loops; i++) _loop()]);
  }

  /// Stop taking jobs. [run] ends when the running jobs are done.
  void stop() {
    _stopping = true;
    if (!_wake.isCompleted) _wake.complete();
  }

  Future<void> _loop() async {
    while (!_stopping) {
      ReservedJob? job;
      try {
        job = await _next();
      } catch (error, stack) {
        Logger.danger('Queue: can not take a job: $error', '$stack');
        await _pause(options.sleep);
        continue;
      }
      if (job == null) {
        if (options.stopWhenEmpty || options.once) return;
        await _pause(options.sleep);
        continue;
      }
      try {
        await process(job);
      } catch (error, stack) {
        Logger.danger('Queue: ${job.payload.job} error: $error', '$stack');
      }
      if (options.once) return;
    }
  }

  Future<ReservedJob?> _next() async {
    for (String queue in options.queues) {
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
      Logger.danger('Queue: failed $label: $error');
      await driver.fail(reserved, '$error\n$stack');
      return;
    }
    int tries = job.tries ?? options.tries;
    if (reserved.attempts > tries) {
      // a worker took it before and did not finish (it crashed or the
      // reservation timed out)
      StateError error = StateError(
        '$label was tried ${reserved.attempts - 1} times and did not finish. '
        'The worker may have crashed, or the job ran longer than retryAfter.',
      );
      await _fail(reserved, job, label, error, StackTrace.current);
      return;
    }
    Duration timeout = job.timeout ?? options.timeout;
    Stopwatch watch = Stopwatch()..start();
    Logger.info('Queue: running $label');
    try {
      await job.handle().timeout(timeout);
    } catch (error, stack) {
      Object reason = error is TimeoutException
          ? TimeoutException('$label timed out after $timeout', timeout)
          : error;
      if (reserved.attempts >= tries) {
        await _fail(reserved, job, label, reason, stack);
        return;
      }
      Duration wait = backoffFor(job, reserved.attempts);
      Logger.warn(
        'Queue: $label failed try ${reserved.attempts} of $tries, '
        'next try in $wait: $reason',
      );
      await driver.release(reserved, wait);
      return;
    }
    await driver.delete(reserved);
    Logger.success('Queue: done $label in ${watch.elapsedMilliseconds}ms');
  }

  /// the wait before the next try, after try number [attempts]
  static Duration backoffFor(DoxJob job, int attempts) {
    List<Duration> backoff = job.backoff;
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
    return codec.decode(jsonDecode(payload.data) as Map<String, Object?>);
  }

  Future<void> _fail(
    ReservedJob reserved,
    DoxJob job,
    String label,
    Object error,
    StackTrace stack,
  ) async {
    Logger.danger('Queue: failed $label: $error');
    await driver.fail(reserved, '$error\n$stack');
    try {
      await job.failed(error, stack);
    } catch (hookError, hookStack) {
      Logger.danger(
        'Queue: failed() of $label threw: $hookError',
        '$hookStack',
      );
    }
  }
}
