import 'dart:convert';

import 'package:dox_core/dox_core.dart' show AESEncryptor, AfterResponse, Dox;
import 'package:dox_mailer/dox_mailer.dart'
    show ArrayTransport, Mail, MailQueue;
import 'package:dox_orm/dox_orm.dart' show DBDriver, QueryBuilder;

import 'job.dart';
import 'job_registry.dart';
import 'job_run.dart';
import 'queue_config.dart';
import 'queue_driver.dart';
import 'queue_fake.dart';
import 'queue_log.dart';
import 'send_queued_mail.dart';

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

  /// the drivers of the named connections of [QueueConfig.connections]
  static final Map<String, QueueDriver> _drivers = <String, QueueDriver>{};
  static QueueFake? _fake;
  static MailQueue? _mailQueueBeforeFake;

  /// true when `Mail.fake()` was on at [fake]. Then [fake] and [restore]
  /// leave the mail queue alone (see [fake]).
  static bool _mailFakeWasOn = false;

  /// Keep dispatched jobs in a [QueueFake] and run or save none of them,
  /// so a test can check what was pushed. It needs no [configure]. It
  /// also catches `Mail.queue()` and `Mail.later()` of dox_mailer.
  /// [dispatchSync] and [dispatchAfterResponse] still run the job. The
  /// fake keeps their jobs in `dispatchedSync` and
  /// `dispatchedAfterResponse`. [restore] undoes it.
  ///
  /// When `Mail.fake()` is already on, mails go to that fake, so this
  /// does not change the mail queue. A `Mail.useQueue()` queue is kept.
  /// ```
  /// QueueFake queue = Queue.fake();
  /// await signUp('ann@example.com');
  /// queue.assertPushed<SendWelcomeEmail>(
  ///   (SendWelcomeEmail job) => job.userId == 1,
  /// );
  /// ```
  static QueueFake fake() {
    QueueFake fake = QueueFake();
    // keep the mail queue set before the fake (a second fake() keeps the first)
    if (_fake == null) {
      _mailFakeWasOn = _mailFakeOn();
      _mailQueueBeforeFake = _mailFakeWasOn ? null : _currentMailQueue();
    }
    _fake = fake;
    // while Mail.fake() is on, Mail.mailQueue gives that fake, so the
    // queue of Mail.useQueue() can not be read back: do not replace it
    if (!_mailFakeWasOn) Mail.useQueue(const QueueMailQueue());
    return fake;
  }

  /// Stop the fake: [dispatch] uses the config again. `Mail.queue()` gets
  /// the queue it had before [fake] (also one from `Mail.useQueue()`). If
  /// it had none, it uses this queue when [configure] was called, else it
  /// has no queue.
  static void restore() {
    MailQueue? before = _mailQueueBeforeFake;
    bool keep = _mailFakeWasOn;
    _fake = null;
    _mailQueueBeforeFake = null;
    _mailFakeWasOn = false;
    if (keep) return;
    Mail.useQueue(before ?? (_config == null ? null : const QueueMailQueue()));
  }

  /// the queue of `Mail.useQueue()`, or null when there is none
  static MailQueue? _currentMailQueue() {
    try {
      return Mail.mailQueue;
    } on StateError {
      return null;
    }
  }

  /// `Mail.fake()` is on: `Mail.mailQueue` gives its [ArrayTransport]
  static bool _mailFakeOn() => _currentMailQueue() is ArrayTransport;

  /// Use [config] in this isolate. `QueueProvider` calls it. Call it in
  /// tests, eg. with the `sync` driver.
  ///
  /// It also lets `Mail.queue()` of dox_mailer put mails on this queue.
  static void configure(QueueConfig config) {
    _config = config;
    _driver = null;
    _drivers.clear();
    Mail.useQueue(const QueueMailQueue());
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

  /// The job store of the queue connection [connection] (a name of
  /// [QueueConfig.connections]), made on first use. Null: [driver].
  /// Throws an [ArgumentError] for an unknown name, and for a `sync`
  /// connection (it has no store).
  static QueueDriver driverOf(String? connection) {
    if (connection == null) return driver;
    QueueConfig settings = config.connectionOf(connection);
    return _drivers[connection] ??= settings.createDriver(name: connection);
  }

  /// Put [job] on the queue and return its id. [queue] wins over
  /// `job.queue`. With a [delay], workers take it after the delay. With
  /// [delayUntil], workers take it at that time (a past time: at once).
  /// Give [delay] or [delayUntil], not both. With the `sync` driver the
  /// job runs at once (no delay).
  ///
  /// Opt-in [connection]: a name of [QueueConfig.connections], like
  /// Laravel `onConnection()`. Null: the main connection.
  ///
  /// Opt-in [afterCommit]: inside a transaction of the default dox_orm
  /// connection, the job is pushed (or run, with `sync`) only after that
  /// transaction commits. A rollback drops it. Outside a transaction it
  /// is pushed at once. Null: [QueueConfig.afterCommit]. `false` wins
  /// over the setting, like Laravel `beforeCommit()`. The job id is
  /// known at once, so it is returned at once.
  static Future<String> dispatch(
    DoxJob job, {
    Duration? delay,
    String? queue,
    DateTime? delayUntil,
    String? connection,
    bool? afterCommit,
  }) async {
    if (delayUntil != null) {
      if (delay != null) {
        throw ArgumentError('Give delay or delayUntil, not both.');
      }
      Duration wait = delayUntil.difference(DateTime.now());
      delay = wait.isNegative ? Duration.zero : wait;
    }
    QueueFake? fake = _fake;
    if (fake != null) {
      fake.pushed.add(
        PushedJob(
          job,
          queue: queue ?? job.queue,
          delay: delay,
          connection: connection,
        ),
      );
      return JobPayload.newId();
    }
    JobRegistry jobs = config.jobs;
    QueueConfig settings = config.connectionOf(connection);
    String id = JobPayload.newId();
    if ((afterCommit ?? config.afterCommit) && DBDriver.inTransaction) {
      // a wrong job or a value that is not JSON throws now, not at commit
      JobPayload? payload = settings.driver != 'sync' || settings.serializeSync
          ? _payload(job, id, queue, jobs)
          : null;
      // ponytail: waits for the default dox_orm connection only
      await QueryBuilder.afterCommit(
        () => _send(job, payload, settings, connection, delay),
      );
      return id;
    }
    if (settings.driver == 'sync') {
      if (!settings.serializeSync) {
        await _runNow(job);
        return id;
      }
      await _send(
        job,
        _payload(job, id, queue, jobs),
        settings,
        connection,
        delay,
      );
      return id;
    }
    JobPayload payload = _payload(job, id, queue, jobs);
    await _send(job, payload, settings, connection, delay);
    return id;
  }

  /// Push [payload], or run the job with the `sync` driver. [payload] is
  /// null only for `sync` without `serializeSync`: then [job] runs as it is.
  static Future<void> _send(
    DoxJob job,
    JobPayload? payload,
    QueueConfig settings,
    String? connection,
    Duration? delay,
  ) async {
    if (settings.driver == 'sync') {
      if (payload == null) return _runNow(job);
      // save and read back, like a worker gets the job
      JobPayload saved = JobPayload.decode(payload.encode());
      return _runNow(_decode(config.jobs, saved));
    }
    await driverOf(connection).push(payload!, delay: delay ?? Duration.zero);
  }

  /// [job] as it is saved on the queue
  static JobPayload _payload(
    DoxJob job,
    String id,
    String? queue,
    JobRegistry jobs,
  ) {
    JobCodec codec = jobs.of(job);
    String data = jsonEncode(codec.encode(job), toEncodable: _toJson);
    if (job is ShouldBeEncrypted) data = encryptData(data, codec.name);
    return JobPayload(
      id: id,
      job: codec.name,
      queue: queue ?? job.queue,
      data: data,
      retryUntil: job.retryUntil?.millisecondsSinceEpoch,
    );
  }

  /// the job of a saved [payload]. Throws when its name is not in [jobs]
  /// or its data can not be read.
  static DoxJob _decode(JobRegistry jobs, JobPayload payload) {
    JobCodec? codec = jobs.byName(payload.job);
    if (codec == null) {
      throw StateError(
        'No job class named ${payload.job} in the job registry.',
      );
    }
    String data = readData(payload.data, payload.job);
    return codec.decode(jsonDecode(data) as Map<String, Object?>);
  }

  /// Run [job] now, in this process, with no queue. When it throws,
  /// `job.failed()` runs and the error is thrown again.
  /// With [fake] on, the job still runs and is kept in
  /// `QueueFake.dispatchedSync`.
  static Future<void> dispatchSync(DoxJob job) {
    _fake?.dispatchedSync.add(job);
    return _runNow(job);
  }

  /// Run [job] in this process after the response is sent. See
  /// `DoxJob.dispatchAfterResponse()`.
  /// With [fake] on, the job still runs and is kept in
  /// `QueueFake.dispatchedAfterResponse`.
  static void dispatchAfterResponse(DoxJob job) {
    _fake?.dispatchedAfterResponse.add(job);
    AfterResponse.run(() => _runNow(job));
  }

  /// [dispatchSync] without the fake: the job runs, nothing is kept.
  /// A `fail()` inside `handle()` calls `failed()`. `release()` and
  /// `delete()` do nothing here: there is no queue.
  static Future<void> _runNow(DoxJob job) async {
    JobRun run = JobRun(1);
    try {
      await run.handle(job);
    } catch (error, stack) {
      // a fail() call wins over an error thrown after it, like in a
      // worker. The caller still gets the thrown error.
      if (run.action == JobAction.fail) {
        await job.failed(run.error!, run.stack!);
      } else {
        await job.failed(error, stack);
      }
      rethrow;
    }
    if (run.action == JobAction.fail) await job.failed(run.error!, run.stack!);
  }

  /// Close the job stores of this isolate (also those of
  /// [QueueConfig.connections]). The next use opens them again.
  static Future<void> close() async {
    List<QueueDriver?> drivers = <QueueDriver?>[_driver, ..._drivers.values];
    _driver = null;
    _drivers.clear();
    for (QueueDriver? driver in drivers) {
      await driver?.close();
    }
  }

  /// the key purpose of job data, so a value encrypted for something
  /// else (eg. a cookie) can not be read as job data
  static const String encryptPurpose = 'dox:queue';

  /// Encrypt the JSON [data] of a [ShouldBeEncrypted] job with the app
  /// key. The job name [job] is bound to it, so it can not be moved to
  /// another job. The output is base64, so it never starts with `{`.
  static String encryptData(String data, String job) {
    return AESEncryptor.encode(
      data,
      Dox().config.appKey,
      associatedData: job,
      purpose: encryptPurpose,
    );
  }

  /// The JSON data of a payload. Plain JSON starts with `{`, everything
  /// else was made by [encryptData]. Throws a `DecryptException` when the
  /// key is wrong or the data was changed.
  static String readData(String data, String job) {
    if (data.startsWith('{')) return data;
    return AESEncryptor.decode(
      data,
      Dox().config.appKey,
      associatedData: job,
      purpose: encryptPurpose,
      previousSecrets: Dox().config.previousKeys,
    );
  }

  /// `retryUntil` of the job class of [payload], read now, as unix time
  /// in milliseconds. Null when the job has none, or can not be read
  /// (the worker then fails it with the real error).
  static int? retryUntilOf(JobPayload payload) {
    try {
      JobCodec? codec = config.jobs.byName(payload.job);
      if (codec == null) return null;
      String data = readData(payload.data, payload.job);
      DoxJob job = codec.decode(jsonDecode(data) as Map<String, Object?>);
      return job.retryUntil?.millisecondsSinceEpoch;
    } catch (error) {
      // only the error type: the error text can quote the job data, and
      // the data of a `ShouldBeEncrypted` job must stay secret
      QueueLog.warn(
        'Queue: can not read retryUntil of ${payload.job} '
        '(${payload.id}): ${error.runtimeType}',
      );
      return null;
    }
  }

  /// The failed job [text] (a [JobPayload.encode] text) for `queue:retry`.
  /// A job with a `retryUntil` gets a new one, read again from the job
  /// class. The drivers call it.
  static JobPayload retriedPayload(String text) {
    JobPayload payload = JobPayload.decode(text);
    return payload.retried(
      retryUntil: payload.retryUntil == null ? null : retryUntilOf(payload),
    );
  }

  /// the job data has only JSON types, dates and enums
  static Object? _toJson(Object? value) {
    if (value is DateTime) return value.toIso8601String();
    if (value is Enum) return value.name;
    throw UnsupportedError('$value (${value.runtimeType}) is not JSON');
  }
}
