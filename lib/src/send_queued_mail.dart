import 'package:dox_core/dox_core.dart' show AESEncryptor, Dox;
import 'package:dox_mailer/dox_mailer.dart'
    show Mail, MailException, MailQueue, Message;

import 'job.dart';
import 'job_registry.dart';
import 'queue.dart';

/// The built-in job of `Mail.queue()` and `Mail.later()` of dox_mailer.
/// It sends a saved [message] with the mailer [mailer].
///
/// Apps do not use it. It is not in `lib/generated/jobs.g.dart`: the
/// [JobRegistry] finds it by itself (see [JobRegistry.builtIn]).
class SendQueuedMail extends DoxJob {
  SendQueuedMail({required this.mailer, required this.message});

  /// the mailer name of the mail config, eg. `smtp`
  final String mailer;

  /// the message, from `Message.toJson()`
  final Map<String, Object?> message;

  /// the saved name. The `dox_queue:` start can not clash with an app job.
  static const String jobName = 'dox_queue:SendQueuedMail';

  static const JobCodec codec = JobCodec(
    jobName,
    SendQueuedMail,
    encode,
    _decode,
  );

  @override
  Future<void> handle() async {
    await Mail.mailer(mailer).send(Message.fromJson(message));
  }

  static Map<String, Object?> encode(DoxJob job) {
    SendQueuedMail value = job as SendQueuedMail;
    return <String, Object?>{'mailer': value.mailer, 'message': value.message};
  }

  static DoxJob _decode(Map<String, Object?> data) {
    return SendQueuedMail(
      mailer: data['mailer'] as String,
      message: Map<String, Object?>.from(
        data['message'] as Map<Object?, Object?>,
      ),
    );
  }
}

/// [SendQueuedMail] with its data saved encrypted, for
/// `Mail.queue(message, encrypt: true)`.
class SendEncryptedQueuedMail extends SendQueuedMail with ShouldBeEncrypted {
  SendEncryptedQueuedMail({required super.mailer, required super.message});

  static const String jobName = 'dox_queue:SendEncryptedQueuedMail';

  static const JobCodec codec = JobCodec(
    jobName,
    SendEncryptedQueuedMail,
    SendQueuedMail.encode,
    _decode,
  );

  /// Sends the mail. When the send fails, it throws a [RedactedMailError]
  /// in place of the error. An SMTP reply or a [MailException] can name
  /// an address, and the worker writes the error to its log and to the
  /// failed jobs in plain text.
  @override
  Future<void> handle() async {
    try {
      await super.handle();
    } catch (error, stack) {
      Error.throwWithStackTrace(RedactedMailError.of(error), stack);
    }
  }

  static DoxJob _decode(Map<String, Object?> data) {
    return SendEncryptedQueuedMail(
      mailer: data['mailer'] as String,
      message: Map<String, Object?>.from(
        data['message'] as Map<Object?, Object?>,
      ),
    );
  }
}

/// The error of a mail of `Mail.queue(message, encrypt: true)` that
/// could not be sent. The worker logs and saves it in place of the
/// first error, so no address or text of the mail is written in plain
/// text.
///
/// Its text has only the type of the first error and the SMTP or HTTP
/// code. The full text of the first error (and the answer of the server,
/// `MailException.body`) is encrypted with the app key, like the job
/// data. At most the first 64K characters (UTF-16 code units) are
/// encrypted, so a huge error does not fill the log. Read it with
/// [decrypt].
/// ```
/// RedactedMailError: MailException (code 550). Details are encrypted: <base64>
/// ```
class RedactedMailError implements Exception {
  RedactedMailError(this.type, {this.statusCode, this.details});

  /// [error] with its text encrypted. When that fails, only the type
  /// (and the code) is kept, never the text. Everything is read from
  /// [error] inside the `try`, so an error that throws can not leak.
  factory RedactedMailError.of(Object error) {
    String type = 'unknown';
    int? code;
    String? details;
    try {
      type = '${error.runtimeType}';
      code = error is MailException ? error.statusCode : null;
      String text = '$error';
      if (error is MailException && (error.body ?? '').isNotEmpty) {
        text = '$text\nbody: ${error.body}';
      }
      details = Queue.encryptData(_cut(text), _purpose);
    } catch (_) {
      details = null;
    }
    return RedactedMailError(type, statusCode: code, details: details);
  }

  /// the most characters (UTF-16 code units) of the error that are
  /// encrypted
  static const int maxDetails = 64 * 1024;

  /// the first [maxDetails] characters of [text]. It does not cut an
  /// emoji (a surrogate pair) in two.
  static String _cut(String text) {
    if (text.length <= maxDetails) return text;
    int end = maxDetails;
    int last = text.codeUnitAt(end - 1);
    if (last >= 0xD800 && last <= 0xDBFF) end--;
    return text.substring(0, end);
  }

  /// bound to the encrypted details, so job data can not be read as them
  static const String _purpose = 'dox_queue:RedactedMailError';

  static const String _marker = 'Details are encrypted: ';

  /// the class of the first error, eg. `MailException`
  final String type;

  /// the SMTP reply code or the HTTP status, when there is one
  final int? statusCode;

  /// the encrypted text of the first error, or null when it could not
  /// be encrypted
  final String? details;

  /// The text of the first error, from a saved or logged [error] text
  /// (eg. the `error` of a failed job). Needs the same app key, or one
  /// of `AppConfig.previousKeys`. Null when it has no details. Throws a
  /// `DecryptException` when the key is wrong, or the details are not
  /// real encrypted details.
  /// ```
  /// for (FailedJob job in await Queue.driver.failed()) {
  ///   print(RedactedMailError.decrypt(job.error));
  /// }
  /// ```
  static String? decrypt(String error) {
    int at = error.indexOf(_marker);
    if (at < 0) return null;
    String rest = error.substring(at + _marker.length);
    String details = rest.split(RegExp(r'\s')).first;
    return AESEncryptor.decode(
      details,
      Dox().config.appKey,
      associatedData: _purpose,
      purpose: Queue.encryptPurpose,
      previousSecrets: Dox().config.previousKeys,
    );
  }

  @override
  String toString() {
    String code = statusCode == null ? '' : ' (code $statusCode)';
    String text = details == null
        ? 'details could not be encrypted'
        : '$_marker$details';
    return 'RedactedMailError: $type$code. $text';
  }
}

/// Puts the mails of `Mail.queue()` on this queue. `Queue.configure()`
/// sets it with `Mail.useQueue()`.
class QueueMailQueue implements MailQueue {
  const QueueMailQueue();

  @override
  Future<String> push(
    String mailer,
    Message message, {
    Duration? delay,
    String? queue,
    bool encrypt = false,
  }) {
    Map<String, Object?> data = message.toJson();
    DoxJob job = encrypt
        ? SendEncryptedQueuedMail(mailer: mailer, message: data)
        : SendQueuedMail(mailer: mailer, message: data);
    return Queue.dispatch(job, delay: delay, queue: queue);
  }
}
