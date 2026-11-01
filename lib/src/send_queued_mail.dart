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
      Error.throwWithStackTrace(RedactedMailError.of(error, message), stack);
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
/// could not be sent, with the mail data taken out (see [redact]). The
/// type of the first error and the SMTP or HTTP code stay, to help find
/// the problem.
class RedactedMailError implements Exception {
  RedactedMailError(this.type, this.message, {this.statusCode});

  /// [error] with the data of [mail] (from `Message.toJson()`) taken out.
  /// When that fails, the text is only the type, never the first text.
  factory RedactedMailError.of(Object error, Map<String, Object?> mail) {
    String type = '${error.runtimeType}';
    int? code = error is MailException ? error.statusCode : null;
    String text;
    try {
      text = redact('$error', mail);
    } catch (_) {
      text = '$type: (could not redact)';
    }
    return RedactedMailError(type, text, statusCode: code);
  }

  /// the class of the first error, eg. `MailException`
  final String type;

  /// the text of the first error, with `***` for the mail data
  final String message;

  /// the SMTP reply code or the HTTP status, when there is one
  final int? statusCode;

  /// any email address, also one that is not in the mail. The length
  /// limits (as in the email RFC) keep it fast on a long text.
  static final RegExp _email = RegExp(
    r'''[^\s<>()\[\]"',;:@]{1,64}@[^\s<>()\[\]"',;:@]{1,255}''',
  );

  static final RegExp _wordChar = RegExp(r'[\p{L}\p{N}_]', unicode: true);

  /// [text] with `***` for:
  /// - every email address,
  /// - every whole text of [mail]: names, subject, body, header and
  ///   metadata values, file names. Case does not count.
  ///
  /// A text must be a whole word in [text], so the name `Ann` does not
  /// break `Annual`. Texts shorter than 3 letters and tags stay (tags
  /// are labels like `smtp`, so they would hide useful words). A part of
  /// a text, or an encoded form (JSON, MIME, URL), is not found.
  static String redact(String text, Map<String, Object?> mail) {
    List<String> secrets = <String>[];
    _collect(mail, secrets);
    secrets.sort(_longestFirst);
    String result = text;
    for (String secret in secrets) {
      // a text longer than the error can not be in it
      if (secret.length > result.length) continue;
      result = _hide(result, secret);
    }
    return result.replaceAll(_email, '***');
  }

  /// [text] with `***` for each whole-word [secret], found with a plain
  /// search (a RegExp of a long body is too large for Dart)
  static String _hide(String text, String secret) {
    String lower = text.toLowerCase();
    String needle = secret.toLowerCase();
    // ponytail: rare letters change length in lower case. Then the
    // places do not match, so work on the lower case text.
    String source = lower.length == text.length ? text : lower;
    bool wordStart = _isWord(needle, 0);
    bool wordEnd = _isWord(needle, needle.length - 1);
    StringBuffer out = StringBuffer();
    int start = 0;
    int search = 0;
    while (true) {
      int at = lower.indexOf(needle, search);
      if (at < 0) break;
      int end = at + needle.length;
      if ((wordStart && _isWord(lower, at - 1)) ||
          (wordEnd && _isWord(lower, end))) {
        search = at + 1;
        continue;
      }
      out
        ..write(source.substring(start, at))
        ..write('***');
      start = end;
      search = end;
    }
    out.write(source.substring(start));
    return out.toString();
  }

  /// true when [text] has a letter, digit or `_` at [index]
  static bool _isWord(String text, int index) {
    if (index < 0 || index >= text.length) return false;
    return _wordChar.hasMatch(text[index]);
  }

  /// every text in [value], but not the file bytes, content types and tags
  static void _collect(Object? value, List<String> into, [String? key]) {
    if (key == 'bytes' || key == 'contentType' || key == 'tags') return;
    if (value is String) {
      String secret = value.trim();
      if (secret.length >= 3) into.add(secret);
    } else if (value is Map) {
      for (MapEntry<Object?, Object?> entry in value.entries) {
        _collect(entry.value, into, '${entry.key}');
      }
    } else if (value is List) {
      for (Object? item in value) {
        _collect(item, into);
      }
    }
  }

  static int _longestFirst(String a, String b) => b.length - a.length;

  @override
  String toString() {
    String code = statusCode == null ? '' : ' (code $statusCode)';
    return 'RedactedMailError$code: $message';
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
