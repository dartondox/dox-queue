import 'package:ioredis/ioredis.dart';

import 'drivers/database_queue_driver.dart';
import 'drivers/redis_queue_driver.dart';
import 'job_registry.dart';
import 'queue_driver.dart';

/// Settings of the queue. Put it in `lib/config/queue.dart`.
/// ```
/// QueueConfig queueConfig = QueueConfig(
///   driver: 'redis',
///   jobs: jobRegistry,
///   redis: RedisOptions(host: 'localhost', port: 6379),
/// );
/// ```
class QueueConfig {
  QueueConfig({
    this.driver = 'sync',
    this.jobs = const JobRegistry(<JobCodec>[]),
    this.redis,
    this.prefix = 'dox_queue:',
    this.connection,
    this.retryAfter = const Duration(seconds: 90),
    this.workInProcess = false,
    this.workerQueues = const <String>['default'],
    this.workerConcurrency = 1,
  });

  /// `sync`, `redis` or `database`.
  /// - `sync`: `dispatch()` runs the job at once. No worker. Good for
  ///   tests and local work.
  /// - `redis`: jobs are in Redis.
  /// - `database`: jobs are in the `jobs` table (see `QueueTables`).
  final String driver;

  /// every job class: `jobRegistry` from `lib/generated/jobs.g.dart`
  final JobRegistry jobs;

  /// the Redis server, for the `redis` driver. Null: localhost:6379.
  final RedisOptions? redis;

  /// the start of every Redis key
  final String prefix;

  /// the dox_orm connection name, for the `database` driver.
  /// Null: the default connection.
  final String? connection;

  /// When a worker took a job and did not finish it in this time (eg. it
  /// crashed), another worker can take it. Keep it bigger than the
  /// longest job timeout, or a slow job runs twice.
  final Duration retryAfter;

  /// Run a worker inside the web server (in the first isolate), so a
  /// small app needs no `bin/queue.dart` process. Heavy jobs then slow
  /// down the web requests. Use a worker process when jobs are heavy.
  final bool workInProcess;

  /// the queues of the in-process worker, first one first
  final List<String> workerQueues;

  /// jobs at once in the in-process worker
  final int workerConcurrency;

  /// the driver of [driver]. Throws an [ArgumentError] for `sync` and
  /// unknown names.
  QueueDriver createDriver() {
    switch (driver) {
      case 'redis':
        return RedisQueueDriver(
          Redis(redis ?? RedisOptions()),
          prefix: prefix,
          retryAfter: retryAfter,
        );
      case 'database':
        return DatabaseQueueDriver(
          connection: connection,
          retryAfter: retryAfter,
        );
    }
    throw ArgumentError.value(
      driver,
      'driver',
      'has no queue store. Use redis or database',
    );
  }
}
