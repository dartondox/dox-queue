part of 'queue.dart';

/// the error of the job that failed, for a [CatchesError] job
final Expando<Object> _caughtErrors = Expando<Object>('dox_queue error');

/// Opt-in, for a catch job of a chain or a batch: [caughtError] is the
/// error of the job that failed.
/// ```
/// @Job()
/// class TellAdmin extends DoxJob with CatchesError {
///   @override
///   Future<void> handle() async {
///     await alert('the import failed: $caughtError');
///   }
/// }
/// ```
mixin CatchesError on DoxJob {
  /// the error of the job that failed, or null when this job does not
  /// run as a catch job
  Object? get caughtError => _caughtErrors[this];
}

/// Jobs that run one after the other. Make it with `Queue.chain()`.
///
/// The rest of the chain is saved in the payload of the job that runs
/// now, so it works across worker processes and restarts. A catch job
/// is a job too, not a function: a function can not be saved. It runs
/// in the worker where the job failed, like `dispatchSync()`.
class PendingChain {
  /// Throws an [ArgumentError] when [jobs] is empty.
  PendingChain(List<DoxJob> jobs) : jobs = List<DoxJob>.of(jobs) {
    if (this.jobs.isEmpty) {
      throw ArgumentError.value(jobs, 'jobs', 'needs at least one job');
    }
  }

  /// the jobs, first one first
  final List<DoxJob> jobs;

  /// the job of [catchError], or null
  DoxJob? catchJob;

  /// Run [job] when a job of the chain fails for good, like Laravel
  /// `catch()`. It runs once, in the process where the job failed, with
  /// no retry. Add [CatchesError] to it to read the error.
  PendingChain catchError(DoxJob job) {
    catchJob = job;
    return this;
  }

  /// Push the first job and return its id. [queue] and [connection]
  /// count for every job of the chain. [delay] counts for the first job
  /// only. With the `sync` driver the jobs run now, one after the other.
  ///
  /// When the first job is a [ShouldBeUnique] job that is skipped, the
  /// id is empty (`''`) and nothing runs.
  Future<String> dispatch({
    Duration? delay,
    String? queue,
    String? connection,
  }) async {
    QueueFake? fake = Queue._fake;
    if (fake != null) {
      fake.chains.add(this);
      return JobPayload.newId();
    }
    QueueConfig config = Queue.config;
    if (config.connectionOf(connection).driver == 'sync') {
      return _runNow(queue, connection);
    }
    JobRegistry registry = config.jobs;
    List<Map<String, Object?>> rest = <Map<String, Object?>>[
      for (DoxJob job in jobs.skip(1)) Queue._item(job, queue, registry),
    ];
    DoxJob? onError = catchJob;
    return Queue._dispatch(
      jobs.first,
      delay: delay,
      queue: queue,
      connection: connection,
      chain: rest.isEmpty ? null : rest,
      chainCatch: onError == null ? null : Queue._item(onError, null, registry),
    );
  }

  /// the `sync` driver: run the jobs now. A job that throws runs the
  /// catch job, then the error is thrown again, like `dispatch()`.
  Future<String> _runNow(String? queue, String? connection) async {
    String first = '';
    for (DoxJob job in jobs) {
      String id;
      try {
        id = await Queue.dispatch(job, queue: queue, connection: connection);
      } catch (error) {
        await Queue._callback(catchJob, error: error);
        rethrow;
      }
      if (id.isEmpty) {
        await Queue._callback(catchJob, error: JobBus._skipped(job));
        return first;
      }
      if (job == jobs.first) first = id;
    }
    return first;
  }
}

/// What the worker does for chains when a job ends. Not exported.
class JobBus {
  /// The job of [payload] is done: push the next job of its chain.
  /// An error here is logged: the job itself is done.
  static Future<void> done(
    JobPayload payload,
    QueueDriver driver,
    JobRegistry jobs,
  ) async {
    List<Map<String, Object?>>? chain = payload.chain;
    if (chain == null || chain.isEmpty) return;
    try {
      await _next(payload, chain, driver, jobs);
    } catch (error, stack) {
      QueueLog.danger(
        'Queue: can not push the next job of the chain of '
            '${payload.job} ${payload.id}: $error',
        '$stack',
      );
    }
  }

  /// The job of [payload] failed for good: run the catch job of its
  /// chain. The rest of the chain is dropped.
  static Future<void> failed(
    JobPayload payload,
    JobRegistry jobs,
    Object error,
  ) async {
    Map<String, Object?>? onError = payload.chainCatch;
    if (onError == null) return;
    await Queue._callback(Queue._readItem(onError, jobs), error: error);
  }

  static Future<void> _next(
    JobPayload payload,
    List<Map<String, Object?>> chain,
    QueueDriver driver,
    JobRegistry jobs,
  ) async {
    Map<String, Object?> item = chain.first;
    DoxJob? onError = payload.chainCatch == null
        ? null
        : Queue._readItem(payload.chainCatch!, jobs);
    DoxJob next;
    try {
      next = Queue._itemJob(item, jobs);
    } catch (error) {
      // the class is gone or the data can not be read: a retry gives the
      // same error, so the chain stops here
      QueueLog.danger(
        'Queue: can not read ${item['job']}, the next job of a chain: '
        '$error',
      );
      await Queue._callback(onError, error: error);
      return;
    }
    String id = JobPayload.newId();
    List<Map<String, Object?>> rest = chain.sublist(1);
    JobPayload nextPayload = Queue._payload(
      next,
      id,
      item['queue'] as String?,
      jobs,
      chain: rest.isEmpty ? null : rest,
      chainCatch: payload.chainCatch,
    );
    if (!await Queue._send(next, id, nextPayload, driver, null)) {
      await Queue._callback(onError, error: _skipped(next));
    }
  }

  /// the error of a [ShouldBeUnique] job of a chain that was skipped
  static StateError _skipped(DoxJob job) {
    return StateError(
      '${job.runtimeType} of a chain was skipped: a job with the same '
      'unique id is on the queue. The rest of the chain is dropped.',
    );
  }
}
