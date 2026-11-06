import 'dart:convert';
import 'dart:math';

/// One queued job, as it is saved: the job name, its data and how many
/// times a worker took it.
class JobPayload {
  JobPayload({
    required this.id,
    required this.job,
    required this.queue,
    required this.data,
    this.attempts = 0,
    this.retryUntil,
    this.exceptions = 0,
  });

  /// a random id, like `0f3c...`
  final String id;

  /// the job name from the registry
  final String job;
  final String queue;

  /// the constructor parameters as JSON text. It is a string inside the
  /// payload, so Redis scripts can change [attempts] and keep it as it is.
  final String data;

  /// how many times a worker took the job
  final int attempts;

  /// `DoxJob.retryUntil` as unix time in milliseconds, read once at
  /// dispatch. Null: the job has none (also for jobs saved before it).
  final int? retryUntil;

  /// how many tries threw an error, for `DoxJob.maxExceptions`
  final int exceptions;

  /// a new random id: 16 random bytes as hex
  static String newId() {
    Random random = Random.secure();
    StringBuffer out = StringBuffer();
    for (int i = 0; i < 16; i++) {
      out.write(random.nextInt(256).toRadixString(16).padLeft(2, '0'));
    }
    return out.toString();
  }

  /// a copy with other [attempts]
  JobPayload withAttempts(int attempts) {
    return JobPayload(
      id: id,
      job: job,
      queue: queue,
      data: data,
      attempts: attempts,
      retryUntil: retryUntil,
      exceptions: exceptions,
    );
  }

  /// a copy with other [exceptions]
  JobPayload withExceptions(int exceptions) {
    return JobPayload(
      id: id,
      job: job,
      queue: queue,
      data: data,
      attempts: attempts,
      retryUntil: retryUntil,
      exceptions: exceptions,
    );
  }

  /// [retryUntil] and [exceptions] are left out when not set, so the
  /// text of a job with neither is the same as before they existed
  String encode() {
    return jsonEncode(<String, Object?>{
      'id': id,
      'job': job,
      'queue': queue,
      'data': data,
      'attempts': attempts,
      'retryUntil': ?retryUntil,
      if (exceptions > 0) 'exceptions': exceptions,
    });
  }

  static JobPayload decode(String text) {
    Map<String, Object?> map = jsonDecode(text) as Map<String, Object?>;
    return JobPayload(
      id: map['id'] as String,
      job: map['job'] as String,
      queue: map['queue'] as String,
      data: map['data'] as String,
      attempts: (map['attempts'] as num? ?? 0).toInt(),
      retryUntil: (map['retryUntil'] as num?)?.toInt(),
      exceptions: (map['exceptions'] as num? ?? 0).toInt(),
    );
  }
}

/// A job that a worker took from the queue. Other workers do not get it
/// until the worker deletes or releases it, or until `retryAfter` passes
/// (the worker crashed).
class ReservedJob {
  ReservedJob(this.payload, {required this.attempts, required this.ref});

  final JobPayload payload;

  /// how many times a worker took the job, this time included
  final int attempts;

  /// what the driver needs to find the job again: a row id, or the
  /// reserved text in Redis
  final Object ref;
}

/// A job that failed all its tries.
class FailedJob {
  FailedJob({
    required this.id,
    required this.queue,
    required this.payload,
    required this.error,
    required this.failedAt,
  });

  /// the job id, for `retry <id>` and `forget <id>`
  final String id;
  final String queue;

  /// the saved job, as [JobPayload.encode] text
  final String payload;

  /// the error and its stack trace
  final String error;
  final DateTime failedAt;

  /// the job name
  String get job => JobPayload.decode(payload).job;
}

/// Where queued jobs are kept: Redis or a database table.
abstract class QueueDriver {
  /// add a job. With a [delay], workers take it after the delay.
  Future<void> push(JobPayload payload, {Duration delay = Duration.zero});

  /// take the next job of [queue], or null when there is none
  Future<ReservedJob?> pop(String queue);

  /// the job is done: remove it
  Future<void> delete(ReservedJob job);

  /// try the job again after [delay]. Save `job.payload` as it is now:
  /// the worker can give it a new `exceptions` count.
  Future<void> release(ReservedJob job, Duration delay);

  /// the job failed all its tries: move it to the failed jobs
  Future<void> fail(ReservedJob job, String error);

  /// the failed jobs, oldest first
  Future<List<FailedJob>> failed();

  /// put the failed job [id] back on its queue. False when not found.
  Future<bool> retry(String id);

  /// delete the failed job [id]. False when not found.
  Future<bool> forget(String id);

  /// delete all failed jobs
  Future<void> flush();

  /// Delete every job of [queue] that is waiting, delayed or reserved.
  /// Returns how many were deleted. Failed jobs stay. The built-in
  /// drivers have it, a custom driver throws an [UnsupportedError].
  Future<int> clear(String queue) {
    throw UnsupportedError('$runtimeType can not clear a queue');
  }

  /// Delete the failed jobs that failed before [before]. Returns how
  /// many were deleted. The built-in drivers have it, a custom driver
  /// throws an [UnsupportedError].
  Future<int> pruneFailed(DateTime before) {
    throw UnsupportedError('$runtimeType can not prune failed jobs');
  }

  /// close the connection
  Future<void> close() async {}

  /// now as unix time in milliseconds
  static int now() {
    return DateTime.now().millisecondsSinceEpoch;
  }
}
