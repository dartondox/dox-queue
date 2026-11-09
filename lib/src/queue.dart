import 'dart:convert';

import 'package:dox_core/dox_core.dart' show AESEncryptor, Dox;
import 'package:dox_mailer/dox_mailer.dart'
    show ArrayTransport, Mail, MailQueue;

import 'job.dart';
import 'job_registry.dart';
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
  static QueueFake? _fake;
  static MailQueue? _mailQueueBeforeFake;

  /// true when `Mail.fake()` was on at [fake]. Then [fake] and [restore]
  /// leave the mail queue alone (see [fake]).
  static bool _mailFakeWasOn = false;

  /// Keep dispatched jobs in a [QueueFake] and run or save none of them,
  /// so a test can check what was pushed. It needs no [configure]. It
  /// also catches `Mail.queue()` and `Mail.later()` of dox_mailer.
  /// [dispatchSync] still runs the job. [restore] undoes it.
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

  /// Put [job] on the queue and return its id. [queue] wins over
  /// `job.queue`. With a [delay], workers take it after the delay. With
  /// [delayUntil], workers take it at that time (a past time: at once).
  /// Give [delay] or [delayUntil], not both. With the `sync` driver the
  /// job runs at once (no delay).
  static Future<String> dispatch(
    DoxJob job, {
    Duration? delay,
    String? queue,
    DateTime? delayUntil,
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
      fake.pushed.add(PushedJob(job, queue: queue ?? job.queue, delay: delay));
      return JobPayload.newId();
    }
    QueueConfig settings = config;
    String id = JobPayload.newId();
    if (settings.driver == 'sync') {
      await dispatchSync(job);
      return id;
    }
    JobCodec codec = settings.jobs.of(job);
    String data = jsonEncode(codec.encode(job), toEncodable: _toJson);
    if (job is ShouldBeEncrypted) data = encryptData(data, codec.name);
    JobPayload payload = JobPayload(
      id: id,
      job: codec.name,
      queue: queue ?? job.queue,
      data: data,
      retryUntil: job.retryUntil?.millisecondsSinceEpoch,
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
