import 'package:dox_core/dox_core.dart' show Env;
import 'package:ioredis/ioredis.dart';

import 'drivers/database_queue_driver.dart';
import 'drivers/redis_queue_driver.dart';
import 'job_registry.dart';
import 'queue_driver.dart';
import 'worker.dart';

/// Settings of the queue. Put it in `lib/config/queue.dart`.
/// ```
/// QueueConfig queueConfig = QueueConfig(
///   driver: 'redis',
///   jobs: jobRegistry,
///   redis: RedisOptions(host: 'localhost', port: 6379),
/// );
/// ```
/// Opt-in: more queue connections, by name, in [connections]:
/// ```
/// QueueConfig queueConfig = QueueConfig(
///   driver: 'redis',
///   jobs: jobRegistry,
///   connections: <String, QueueConfig>{
///     'database': QueueConfig(driver: 'database'),
///   },
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
    this.workerTimeoutGrace,
    this.serializeSync = false,
    this.connections = const <String, QueueConfig>{},
    this.afterCommit = false,
    this.failedAtTimestamp = false,
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
  /// To read it from the env, set it to [redisFromEnv].
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

  /// Opt-in [WorkerOptions.timeoutGrace] of the in-process worker.
  /// Null (the default): no wait after a timeout.
  final Duration? workerTimeoutGrace;

  /// Opt-in, for the `sync` driver: before the job runs, save it like
  /// a real queue does (the job registry, JSON, encryption) and read it
  /// back. So a test finds a job that is not in the registry, or a field
  /// that is not JSON, and the job runs as a worker would get it.
  /// False (the default): the job object runs as it is.
  final bool serializeSync;

  /// Opt-in: more queue connections, by name, like Laravel
  /// `connections`. This config is the main connection: it is used when
  /// no connection is named. Pick another one with
  /// `dispatch(connection: 'database')` and the worker flag
  /// `--connection=database`.
  ///
  /// Each entry uses only its [driver], [redis], [prefix],
  /// [connection], [retryAfter], [serializeSync] and
  /// [failedAtTimestamp]. The [jobs] of
  /// this config count for all connections.
  ///
  /// Two `database` connections on the same dox_orm [connection] share
  /// the `jobs` and `failed_jobs` tables. Give each its own dox_orm
  /// connection. If they share, `failed --connection=X` lists all failed
  /// jobs of the table.
  final Map<String, QueueConfig> connections;

  /// Opt-in: the default of `dispatch(afterCommit:)` for every dispatch,
  /// like Laravel `after_commit`. True: a job dispatched inside a
  /// transaction of the default dox_orm connection is pushed only after
  /// that transaction commits, and a rollback drops it. False (the
  /// default): the job is pushed at once. Only the main config counts,
  /// not the entries of [connections].
  final bool afterCommit;

  /// Opt-in, for the `database` driver: `failed_jobs.failed_at` is a
  /// timestamp column in UTC, like Laravel. False (the default): it is
  /// a big integer, unix time in milliseconds. It must match the table:
  /// make the table with `QueueTables.up(schema, failedAtTimestamp:
  /// true)`. The driver checks the column once, on the first use of
  /// `failed_jobs`, and throws a [StateError] when it does not match.
  ///
  /// It counts per connection: each entry of [connections] has its own,
  /// as each can use its own table. The Redis driver does not use it.
  final bool failedAtTimestamp;

  /// The config of the queue connection [name]. Null: this config, the
  /// main connection. Throws an [ArgumentError] for an unknown name.
  QueueConfig connectionOf(String? name) {
    if (name == null) return this;
    QueueConfig? config = connections[name];
    if (config == null) {
      throw ArgumentError.value(
        name,
        'connection',
        'is not in QueueConfig.connections',
      );
    }
    return config;
  }

  /// the driver of [driver]. Throws an [ArgumentError] for `sync` and
  /// unknown names. [name] is the name of the queue connection in
  /// [connections] (null: the main connection). The `database` driver
  /// saves it in `failed_jobs.connection`.
  QueueDriver createDriver({String? name}) {
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
          queueConnection: name,
          failedAtTimestamp: failedAtTimestamp,
        );
    }
    throw ArgumentError.value(
      driver,
      'driver',
      'has no queue store. Use redis or database',
    );
  }

  /// Opt-in: Redis options from the env. The queue does not call it on
  /// its own. Use it in the config: `redis: QueueConfig.redisFromEnv()`.
  /// - `REDIS_URL`, like `rediss://user:password@host:6380/0`. `rediss`
  ///   turns on TLS. When it is set, the keys below are not read.
  /// - else `REDIS_HOST`, `REDIS_PORT`, `REDIS_USERNAME`,
  ///   `REDIS_PASSWORD` and `REDIS_TLS` (true, 1, yes or on).
  ///
  /// A key that is not set keeps the [RedisOptions] default, so with no
  /// keys it is `RedisOptions()`: localhost:6379, no password, no TLS.
  /// [values] replaces the env (for tests).
  static RedisOptions redisFromEnv([Map<String, String>? values]) {
    String? url = _env(values, 'REDIS_URL');
    if (url != null) return _redisFromUrl(url);
    String? port = _env(values, 'REDIS_PORT');
    return RedisOptions(
      host: _env(values, 'REDIS_HOST') ?? '127.0.0.1',
      port: port == null ? 6379 : _number('REDIS_PORT', port),
      username: _env(values, 'REDIS_USERNAME'),
      password: _env(values, 'REDIS_PASSWORD'),
      secure: _isOn(_env(values, 'REDIS_TLS')),
    );
  }

  static RedisOptions _redisFromUrl(String url) {
    Uri uri = Uri.parse(url);
    if (uri.scheme != 'redis' && uri.scheme != 'rediss') {
      throw ArgumentError.value(url, 'REDIS_URL', 'must start with redis://');
    }
    String? username;
    String? password;
    if (uri.userInfo.isNotEmpty) {
      int colon = uri.userInfo.indexOf(':');
      String user = colon < 0 ? uri.userInfo : uri.userInfo.substring(0, colon);
      username = user.isEmpty ? null : Uri.decodeComponent(user);
      if (colon >= 0) {
        password = Uri.decodeComponent(uri.userInfo.substring(colon + 1));
      }
    }
    String db = uri.pathSegments.isEmpty ? '' : uri.pathSegments.first;
    return RedisOptions(
      host: uri.host.isEmpty ? '127.0.0.1' : uri.host,
      port: uri.hasPort ? uri.port : 6379,
      username: username,
      password: password,
      db: db.isEmpty ? 0 : _number('REDIS_URL database', db),
      secure: uri.scheme == 'rediss',
    );
  }

  /// a key of [values], or of the env (the real env, then `.env`).
  /// Empty is the same as not set.
  static String? _env(Map<String, String>? values, String key) {
    String? value = values == null ? Env.get<String?>(key) : values[key];
    return value == null || value.isEmpty ? null : value;
  }

  static int _number(String name, String value) {
    int? number = int.tryParse(value);
    if (number == null) {
      throw ArgumentError.value(value, name, 'is not a number');
    }
    return number;
  }

  static bool _isOn(String? value) {
    return <String>['true', '1', 'yes', 'on'].contains(value?.toLowerCase());
  }
}
