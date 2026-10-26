import 'dart:convert';

import 'job.dart';
import 'job_registry.dart';
import 'queue_config.dart';
import 'queue_driver.dart';

/// Puts jobs on the queue. `QueueProvider` sets it up in every isolate.
/// ```
/// await Queue.dispatch(SendWelcomeEmail(userId: 1));
/// await Queue.dispatch(
///   SendWelcomeEmail(userId: 1),
///   delay: Duration(minutes: 5),
///   queue: 'emails',
/// );
/// ```
class Queue {
  static QueueConfig? _config;
  static QueueDriver? _driver;

  /// Use [config] in this isolate. `QueueProvider` calls it. Call it in
  /// tests, eg. with the `sync` driver.
  static void configure(QueueConfig config) {
    _config = config;
    _driver = null;
  }

  /// the config from [configure]
  static QueueConfig get config {
    QueueConfig? config = _config;
    if (config == null) {
      throw StateError(
        'The queue is not set up. Add a class that extends QueueProvider '
        'in lib/ (see the dox_queue README), or call Queue.configure().',
      );
    }
    return config;
  }

  /// the job store of this isolate, made on first use
  static QueueDriver get driver {
    return _driver ??= config.createDriver();
  }

  /// Put [job] on the queue and return its id. [queue] wins over
  /// `job.queue`. With a [delay], workers take it after the delay.
  /// With the `sync` driver the job runs at once (no delay).
  static Future<String> dispatch(
    DoxJob job, {
    Duration? delay,
    String? queue,
  }) async {
    QueueConfig settings = config;
    String id = JobPayload.newId();
    if (settings.driver == 'sync') {
      await dispatchSync(job);
      return id;
    }
    JobCodec codec = settings.jobs.of(job);
    JobPayload payload = JobPayload(
      id: id,
      job: codec.name,
      queue: queue ?? job.queue,
      data: jsonEncode(codec.encode(job), toEncodable: _toJson),
    );
    await driver.push(payload, delay: delay ?? Duration.zero);
    return id;
  }

  /// Run [job] now, in this process, with no queue. When it throws,
  /// `job.failed()` runs and the error is thrown again.
  static Future<void> dispatchSync(DoxJob job) async {
    try {
      await job.handle();
    } catch (error, stack) {
      await job.failed(error, stack);
      rethrow;
    }
  }

  /// Close the job store of this isolate. The next use opens it again.
  static Future<void> close() async {
    QueueDriver? driver = _driver;
    _driver = null;
    await driver?.close();
  }

  /// the job data has only JSON types, dates and enums
  static Object? _toJson(Object? value) {
    if (value is DateTime) return value.toIso8601String();
    if (value is Enum) return value.name;
    throw UnsupportedError('$value (${value.runtimeType}) is not JSON');
  }
}
