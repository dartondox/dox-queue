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
      if (first.isEmpty) first = id;
    }
    return first;
  }
}

/// What the worker does for chains and batches. Not exported.
class JobBus {
  /// the worker read [job] from [payload]: give it its batch id
  static void read(DoxJob job, JobPayload payload) {
    String? batch = payload.batch;
    if (batch != null) _batchIds[job] = batch;
  }

  /// The job of [payload] is done: push the next job of its chain.
  /// The worker does it before it deletes the job. A push that throws
  /// goes to the caller: the try of the job then counts as failed.
  static Future<void> next(
    JobPayload payload,
    QueueDriver driver,
    JobRegistry jobs,
  ) async {
    List<Map<String, Object?>>? chain = payload.chain;
    if (chain != null && chain.isNotEmpty) {
      await _next(payload, chain, driver, jobs);
    }
  }

  /// The job of [payload] is done and deleted: count it as done in its
  /// batch. An error here is logged: the job itself is done.
  static Future<void> done(JobPayload payload, JobRegistry jobs) async {
    String? batch = payload.batch;
    if (batch == null) return;
    // a retried job that failed before: it is no longer failed
    await _count(
      payload,
      batch,
      jobs,
      payload.batchFailed ? _BatchStep.retriedDone : _BatchStep.done,
    );
  }

  /// The job of [payload] failed for good: run the catch job of its
  /// chain (the rest of the chain is dropped), and count it as failed
  /// in its batch. A retried job that fails again is counted once.
  static Future<void> failed(
    JobPayload payload,
    JobRegistry jobs,
    Object error,
  ) async {
    Map<String, Object?>? onError = payload.chainCatch;
    if (onError != null) {
      await Queue._callback(Queue._readItem(onError, jobs), error: error);
    }
    String? batch = payload.batch;
    if (batch != null && !payload.batchFailed) {
      await _count(payload, batch, jobs, _BatchStep.failed, error);
    }
  }

  /// count the job of [payload] in its [batch]; an error is logged
  static Future<void> _count(
    JobPayload payload,
    String batch,
    JobRegistry jobs,
    _BatchStep step, [
    Object? error,
  ]) async {
    try {
      await Batch._record(batch, jobs, step, error: error);
    } catch (error, stack) {
      QueueLog.danger(
        'Queue: can not count ${payload.job} ${payload.id} in its batch '
            '$batch: $error',
        '$stack',
      );
    }
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

/// the batch id of each job of a batch, see [Batchable]
final Expando<String> _batchIds = Expando<String>('dox_queue batch id');

/// Opt-in, for a job of a batch, like Laravel `Batchable`: [batchId]
/// and [batch]. A job can skip its work when the batch was cancelled:
/// ```
/// @Job()
/// class ImportRows extends DoxJob with Batchable {
///   @override
///   Future<void> handle() async {
///     if ((await batch())?.cancelled ?? false) return;
///     ...
///   }
/// }
/// ```
/// The callback jobs of a batch (`then()`, `catchError()`, ...) get the
/// batch too when they use it.
mixin Batchable on DoxJob {
  /// the id of the batch of this job, or null when it is not in a batch
  String? get batchId => _batchIds[this];

  /// The batch of this job as it is now, read from the `job_batches`
  /// table. Null when the job is not in a batch, or with `Queue.fake()`.
  Future<Batch?> batch() async {
    String? id = batchId;
    return id == null ? null : Queue.findBatch(id);
  }
}

/// Jobs that run as one batch. Make it with `Queue.batch()`.
///
/// The batch is saved in the `job_batches` table (see
/// `QueueTables.upBatches`), also with the Redis driver. Workers count
/// the done and failed jobs there. The callbacks are jobs, not
/// functions: a function can not be saved. They run once, in the
/// process where the step happened, like `dispatchSync()`. Add
/// [Batchable] to them to read the batch, and [CatchesError] to the
/// catch job to read the error.
class PendingBatch {
  /// Throws an [ArgumentError] when [jobs] is empty: an empty batch
  /// would never finish.
  PendingBatch(List<DoxJob> jobs) : jobs = List<DoxJob>.of(jobs) {
    if (this.jobs.isEmpty) {
      throw ArgumentError.value(jobs, 'jobs', 'needs at least one job');
    }
  }

  /// the jobs of the batch
  final List<DoxJob> jobs;

  /// the name of [name]. Empty by default.
  String batchName = '';

  /// true after [allowFailures]
  bool allowsFailures = false;

  /// the jobs of [before], [progress], [then], [catchError] and
  /// [whenComplete], or null
  DoxJob? beforeJob;
  DoxJob? progressJob;
  DoxJob? thenJob;
  DoxJob? catchJob;
  DoxJob? completeJob;

  /// a name to find the batch by, eg. in logs
  PendingBatch name(String value) {
    batchName = value;
    return this;
  }

  /// Do not cancel the batch when a job fails. By default the first
  /// failed job cancels it.
  PendingBatch allowFailures([bool allow = true]) {
    allowsFailures = allow;
    return this;
  }

  /// Run [job] at `dispatch()`, after the batch is saved and before its
  /// jobs are pushed.
  PendingBatch before(DoxJob job) {
    beforeJob = job;
    return this;
  }

  /// Run [job] each time a job of the batch is done (and, with
  /// [allowFailures], each time one fails).
  PendingBatch progress(DoxJob job) {
    progressJob = job;
    return this;
  }

  /// Run [job] once, when every job of the batch is done.
  PendingBatch then(DoxJob job) {
    thenJob = job;
    return this;
  }

  /// Run [job] once, when the first job of the batch fails for good.
  PendingBatch catchError(DoxJob job) {
    catchJob = job;
    return this;
  }

  /// Run [job] once, when every job of the batch ran: done or failed,
  /// like Laravel `finally()`.
  PendingBatch whenComplete(DoxJob job) {
    completeJob = job;
    return this;
  }

  /// Save the batch, push its jobs and return the batch. [queue] and
  /// [connection] count for every job, also for jobs of `Batch.add()`.
  /// With the `sync` driver the jobs run now, one after the other.
  Future<Batch> dispatch({String? queue, String? connection}) async {
    String id = JobPayload.newId();
    QueueFake? fake = Queue._fake;
    if (fake != null) {
      fake.batches.add(this);
      return Batch._(
        id,
        batchName,
        jobs.length,
        jobs.length,
        0,
        DateTime.now(),
        null,
        null,
        const <String, Object?>{},
        this,
      );
    }
    QueueConfig config = Queue.config;
    JobRegistry registry = config.jobs;
    // a job that is not in the registry throws now, before the save
    List<JobPayload> payloads = Batch._payloads(id, jobs, queue, registry);
    Map<String, Object?>? item(DoxJob? job) {
      return job == null ? null : Queue._item(job, null, registry);
    }

    Map<String, Object?> options = <String, Object?>{
      'queue': ?queue,
      'connection': ?connection,
      if (allowsFailures) 'allowFailures': true,
      'progress': ?item(progressJob),
      'then': ?item(thenJob),
      'catch': ?item(catchJob),
      'finally': ?item(completeJob),
    };
    try {
      await Batch._db.execute(
        'INSERT INTO ${Batch.table} (id, name, total_jobs, pending_jobs, '
        'failed_jobs, options, created_at) VALUES (@id, @name, @total, '
        '@total, 0, @options, @now)',
        substitutionValues: <String, dynamic>{
          'id': id,
          'name': batchName,
          'total': jobs.length,
          'options': jsonEncode(options),
          'now': QueueDriver.now(),
        },
      );
    } catch (error) {
      throw StateError(
        'Can not save the batch in the ${Batch.table} table. Make the '
        'table with QueueTables.upBatches() in a migration (see the '
        'dox_queue README), in the dox_orm connection of '
        'QueueConfig.connection. The error: $error',
      );
    }
    await Queue._callback(beforeJob, batch: id);
    await Batch._push(id, jobs, payloads, connection, registry);
    return (await Queue.findBatch(id))!;
  }
}

/// A batch as it was when it was read. Read it again with
/// `Queue.findBatch(id)`.
class Batch {
  Batch._(
    this.id,
    this.name,
    this.totalJobs,
    this.pendingJobs,
    this.failedJobs,
    this.createdAt,
    this.cancelledAt,
    this.finishedAt,
    this._options,
    this._fake,
  );

  /// the table of the batches, see `QueueTables.upBatches`
  static const String table = 'job_batches';

  final String id;
  final String name;

  /// every job of the batch, also those of [add]
  final int totalJobs;

  /// The jobs that are not done yet. A failed job counts here too, as
  /// it can be retried, like Laravel.
  final int pendingJobs;

  /// the jobs that failed for good
  final int failedJobs;
  final DateTime createdAt;
  final DateTime? cancelledAt;

  /// when every job ran (done or failed), or when it was cancelled
  final DateTime? finishedAt;

  /// queue, connection, allowFailures and the callback jobs
  final Map<String, Object?> _options;

  /// the batch of `Queue.fake()`, or null
  final PendingBatch? _fake;

  /// the jobs that are done
  int get processedJobs => totalJobs - pendingJobs;

  /// the done jobs in percent, 0 to 100
  int get progress {
    return totalJobs == 0 ? 0 : (processedJobs * 100 / totalJobs).round();
  }

  /// true when every job ran (done or failed), or it was cancelled
  bool get finished => finishedAt != null;

  bool get cancelled => cancelledAt != null;

  /// Cancel the batch. Its jobs still run: a [Batchable] job reads
  /// `cancelled` to skip its work. Does nothing with `Queue.fake()`.
  Future<void> cancel() async {
    if (_fake != null) return;
    await _db.execute(
      'UPDATE $table SET cancelled_at = @now, finished_at = '
      'COALESCE(finished_at, @now) WHERE id = @id AND cancelled_at IS NULL',
      substitutionValues: <String, dynamic>{'id': id, 'now': QueueDriver.now()},
    );
  }

  /// Add [jobs] to the batch and push them, eg. from a job of the batch.
  /// They go on the queue and connection of the batch. Throws a
  /// [StateError] when the batch is finished, cancelled or deleted:
  /// then no job is pushed. With `Queue.fake()` they are added to the
  /// faked batch.
  Future<void> add(List<DoxJob> jobs) async {
    PendingBatch? fake = _fake;
    if (fake != null) {
      fake.jobs.addAll(jobs);
      return;
    }
    if (jobs.isEmpty) return;
    JobRegistry registry = Queue.config.jobs;
    List<JobPayload> payloads = _payloads(
      id,
      jobs,
      _options['queue'] as String?,
      registry,
    );
    Batch? open = await _change(
      id,
      total: jobs.length,
      pending: jobs.length,
      onlyOpen: true,
    );
    if (open == null) {
      throw StateError(
        'Can not add jobs to the batch $id: it is finished, cancelled or '
        'deleted.',
      );
    }
    await _push(
      id,
      jobs,
      payloads,
      _options['connection'] as String?,
      registry,
    );
  }

  /// the dox_orm connection of `QueueConfig.connection`
  static DBDriver get _db {
    return SqlQueryBuilder().getDBDriver(Queue.config.connection);
  }

  static List<JobPayload> _payloads(
    String id,
    List<DoxJob> jobs,
    String? queue,
    JobRegistry registry,
  ) {
    return <JobPayload>[
      for (DoxJob job in jobs)
        Queue._payload(job, JobPayload.newId(), queue, registry, batch: id),
    ];
  }

  /// Push the [jobs] of the batch [id]. A [ShouldBeUnique] job that is
  /// skipped is taken out of the counts. The `sync` driver runs each
  /// job now and counts it as done or failed. When a push throws, the
  /// jobs that were not pushed are taken out of the counts, the batch is
  /// cancelled (so it is finished and can be pruned), and the error is
  /// thrown again. No callback runs for them.
  static Future<void> _push(
    String id,
    List<DoxJob> jobs,
    List<JobPayload> payloads,
    String? connection,
    JobRegistry registry,
  ) async {
    QueueConfig settings = Queue.config.connectionOf(connection);
    QueueDriver? driver = Queue._target(settings, connection);
    for (int i = 0; i < jobs.length; i++) {
      DoxJob job = jobs[i];
      JobPayload payload = payloads[i];
      _batchIds[job] = id;
      if (driver != null) {
        bool sent;
        try {
          sent = await Queue._send(job, payload.id, payload, driver, null);
        } catch (_) {
          // the jobs from here on are not pushed: take them out and
          // cancel the batch, so it is finished and can be pruned
          int left = jobs.length - i;
          Batch? batch = await _change(id, total: -left, pending: -left);
          await batch?.cancel();
          rethrow;
        }
        if (!sent) await _record(id, registry, _BatchStep.skipped);
        continue;
      }
      bool sent;
      try {
        sent = await Queue._send(
          job,
          payload.id,
          settings.serializeSync ? payload : null,
          null,
          null,
        );
      } catch (error) {
        await _record(id, registry, _BatchStep.failed, error: error);
        continue;
      }
      await _record(id, registry, sent ? _BatchStep.done : _BatchStep.skipped);
    }
  }

  /// Count a [step] of the batch [id], then run the callback jobs that
  /// are due. Each count change is one atomic step, so only one worker
  /// sees a count reach its end.
  static Future<void> _record(
    String id,
    JobRegistry registry,
    _BatchStep step, {
    int count = 1,
    Object? error,
  }) async {
    Batch? batch = await switch (step) {
      _BatchStep.done => _change(id, pending: -1),
      // the job failed before and was retried: it is not failed now
      _BatchStep.retriedDone => _change(id, pending: -1, failed: -1),
      _BatchStep.failed => _change(id, failed: 1),
      _BatchStep.skipped => _change(id, total: -count, pending: -count),
    };
    if (batch == null) return;
    Map<String, Object?> options = batch._options;
    bool allow = options['allowFailures'] == true;
    switch (step) {
      case _BatchStep.failed:
        if (batch.failedJobs == 1 && !allow) await batch.cancel();
        if (allow) await _run(options['progress'], registry, id);
        if (batch.failedJobs == 1) {
          await _run(options['catch'], registry, id, error);
        }
      case _BatchStep.done:
      case _BatchStep.retriedDone:
        await _run(options['progress'], registry, id);
        if (batch.pendingJobs == 0) await _run(options['then'], registry, id);
      case _BatchStep.skipped:
        // no job ran: no progress, but it can be the last open job
        if (batch.pendingJobs == 0) await _run(options['then'], registry, id);
    }
    // Every other step lowers pending minus failed, so it hits 0 once. A
    // retried job that is done lowers both: it never ends the batch.
    if (step != _BatchStep.retriedDone &&
        batch.pendingJobs == batch.failedJobs) {
      await _db.execute(
        'UPDATE $table SET finished_at = @now WHERE id = @id AND '
        'finished_at IS NULL',
        substitutionValues: <String, dynamic>{
          'id': id,
          'now': QueueDriver.now(),
        },
      );
      await _run(options['finally'], registry, id);
    }
  }

  /// run the callback job [item] of the batch [id]
  static Future<void> _run(
    Object? item,
    JobRegistry registry,
    String id, [
    Object? error,
  ]) async {
    if (item is! Map<String, Object?>) return;
    DoxJob? job = Queue._readItem(item, registry);
    await Queue._callback(job, error: error, batch: id);
  }

  /// Add the numbers to the counts of the batch [id] and return the new
  /// row. Null when there is no such batch, or with [onlyOpen] when it
  /// is finished or cancelled (then nothing changes).
  static Future<Batch?> _change(
    String id, {
    int total = 0,
    int pending = 0,
    int failed = 0,
    bool onlyOpen = false,
  }) async {
    DBDriver db = _db;
    Map<String, dynamic> values = <String, dynamic>{
      'id': id,
      'total': total,
      'pending': pending,
      'failed': failed,
    };
    String open = onlyOpen
        ? ' AND finished_at IS NULL AND cancelled_at IS NULL'
        : '';
    String update =
        'UPDATE $table SET total_jobs = total_jobs + @total, '
        'pending_jobs = pending_jobs + @pending, '
        'failed_jobs = failed_jobs + @failed WHERE id = @id$open';
    if (db.getName() != Driver.mysql) {
      // one statement: SQLite runs one write at a time, Postgres locks
      // the row
      List<Map<String, dynamic>> rows = await db.query(
        '$update RETURNING *',
        substitutionValues: values,
      );
      return rows.isEmpty ? null : _fromRow(rows.first);
    }
    // MySQL and MariaDB have no UPDATE ... RETURNING. The update locks
    // the row until the commit, so the read sees only this change.
    List<Map<String, dynamic>> rows = await db.transaction(() async {
      await db.execute(update, substitutionValues: values);
      return db.query(
        'SELECT * FROM $table WHERE id = @id',
        substitutionValues: values,
      );
    });
    if (rows.isEmpty) return null;
    Batch batch = _fromRow(rows.first);
    // a closed batch: the update did not match
    if (onlyOpen && (batch.finished || batch.cancelled)) return null;
    return batch;
  }

  static Batch _fromRow(Map<String, dynamic> row) {
    return Batch._(
      _text(row['id']),
      _text(row['name']),
      _int(row['total_jobs']),
      _int(row['pending_jobs']),
      _int(row['failed_jobs']),
      _time(row['created_at'])!,
      _time(row['cancelled_at']),
      _time(row['finished_at']),
      jsonDecode(_text(row['options'])) as Map<String, Object?>,
      null,
    );
  }

  /// unix time in milliseconds
  static DateTime? _time(Object? value) {
    return value == null
        ? null
        : DateTime.fromMillisecondsSinceEpoch(_int(value));
  }

  /// drivers give a number as int, or as text (eg. mysql bigint)
  static int _int(Object? value) {
    return value is int ? value : int.parse('$value');
  }

  /// MariaDB gives JSON looking text of a long text column as a Map
  static String _text(Object? value) {
    if (value is String) return value;
    if (value is Map || value is List) return jsonEncode(value);
    return '$value';
  }
}

/// what happened to a job of a batch
enum _BatchStep {
  /// it is done
  done,

  /// it failed for good before, was retried, and is done now
  retriedDone,

  /// it failed for good
  failed,

  /// a skipped unique job: it was not pushed
  skipped,
}
