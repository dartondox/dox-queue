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

  /// the most letters of an error that are redacted. The rest is cut,
  /// so a huge error can not block the worker for seconds.
  static const int maxLength = 16 * 1024;

  /// the characters that end an email address: space, `<>()[]"',;:@`
  static final Set<int> _notAddress = <int>{
    ...' \t\r\n<>()[]"\',;:@'.codeUnits,
  };

  /// [text] with `***` for:
  /// - every email address,
  /// - every whole text of [mail]: names, subject, body, header names and
  ///   values, metadata keys and values, file names. Case does not count.
  ///
  /// Only the first [maxLength] letters are kept, and the text then says
  /// it was cut. A text of [mail] that starts near the cut and goes past
  /// it is hidden up to the cut.
  ///
  /// A text must be a whole word in [text], so the name `Ann` does not
  /// break `Annual`. Texts shorter than 3 letters and tags stay (tags
  /// are labels like `smtp`, so they would hide useful words). A part of
  /// a text, an encoded form (JSON, MIME, URL) or another Unicode form
  /// (NFC or NFD) is not found.
  ///
  /// It reads [text] once for the texts and once for the addresses, so
  /// a long error with many recipients stays fast. No RegExp is made
  /// from mail data (a RegExp of a long body is too large for Dart).
  static String redact(String text, Map<String, Object?> mail) {
    int fullLength = text.length;
    bool cut = fullLength > maxLength;
    if (cut) text = text.substring(0, maxLength);
    String lower = text.toLowerCase();
    // ponytail: rare letters change length in lower case. Then the
    // places do not match, so work on the lower case text.
    String source = lower.length == text.length ? text : lower;
    List<String> secrets = _secrets(mail);
    String tail = '';
    if (cut) {
      tail = ' ... (cut, $fullLength letters in all)';
      int? at = _cutStart(lower, secrets);
      if (at != null) {
        source = source.substring(0, at);
        lower = lower.substring(0, at);
        tail = '***$tail';
      }
    }
    Map<int, _Texts> byStart = _byStart(secrets, lower.length);
    String result = byStart.isEmpty
        ? source
        : _hideTexts(source, lower, byStart);
    if (result.contains('@')) result = _hideAddresses(result);
    return '$result$tail';
  }

  /// the trimmed, lower case texts of [mail], 3 letters or more
  static List<String> _secrets(Map<String, Object?> mail) {
    List<String> texts = <String>[];
    _collect(mail, texts);
    return <String>[
      for (String value in texts)
        if (value.trim().length >= 3) value.trim().toLowerCase(),
    ];
  }

  /// the [secrets] that fit in [max] letters, by their first 3 letters
  static Map<int, _Texts> _byStart(List<String> secrets, int max) {
    Map<int, _Texts> byStart = <int, _Texts>{};
    for (String secret in secrets) {
      if (secret.length > max) continue;
      byStart.putIfAbsent(_key(secret, 0), _Texts.new).add(secret);
    }
    return byStart;
  }

  /// the first place where a whole-word text of [secrets] starts at the
  /// end of the cut [lower] text and goes past the cut, or null
  static int? _cutStart(String lower, List<String> secrets) {
    int? first;
    for (String needle in secrets) {
      int from = lower.length - needle.length + 1;
      if (from < 0) from = 0;
      // find the start with a short head of the text, then check the rest
      String head = needle.length > 16 ? needle.substring(0, 16) : needle;
      int last = lower.length - head.length;
      int at = lower.indexOf(head, from);
      while (at >= 0 && at <= last && !_startsAt(lower, needle, at)) {
        at = lower.indexOf(head, at + 1);
      }
      if (at < 0 || at > last) {
        at = -1;
        for (int i = last + 1 > from ? last + 1 : from; i < lower.length; i++) {
          if (_startsAt(lower, needle, i)) {
            at = i;
            break;
          }
        }
      }
      if (at >= 0 && (first == null || at < first)) first = at;
    }
    return first;
  }

  /// true when the end of [lower] from [at] is the start of [needle], at
  /// the start of a word
  static bool _startsAt(String lower, String needle, int at) {
    if (_isWord(needle, 0) && _isWord(lower, at - 1)) return false;
    return needle.startsWith(lower.substring(at));
  }

  /// the first 3 code units of [text] at [at], as one number
  static int _key(String text, int at) {
    return (text.codeUnitAt(at) << 32) |
        (text.codeUnitAt(at + 1) << 16) |
        text.codeUnitAt(at + 2);
  }

  /// one pass over [lower]: at each place, the longest whole-word text
  /// of [byStart] that starts there is replaced by `***`
  static String _hideTexts(
    String source,
    String lower,
    Map<int, _Texts> byStart,
  ) {
    _Hash hash = _Hash(lower);
    StringBuffer out = StringBuffer();
    int start = 0;
    int at = 0;
    while (at + 3 <= lower.length) {
      _Texts? texts = byStart[_key(lower, at)];
      int? length = texts?.longestAt(lower, hash, at);
      if (length == null) {
        at++;
        continue;
      }
      out
        ..write(source.substring(start, at))
        ..write('***');
      at += length;
      start = at;
    }
    out.write(source.substring(start));
    return out.toString();
  }

  /// [text] with `***` for every email address, of any length. Each `@`
  /// grows left and right over address letters, so it is one pass.
  static String _hideAddresses(String text) {
    StringBuffer out = StringBuffer();
    int start = 0;
    int at = text.indexOf('@');
    while (at >= 0) {
      int left = at;
      while (left > start && !_notAddress.contains(text.codeUnitAt(left - 1))) {
        left--;
      }
      int right = at + 1;
      while (right < text.length &&
          !_notAddress.contains(text.codeUnitAt(right))) {
        right++;
      }
      if (left < at && right > at + 1) {
        out
          ..write(text.substring(start, left))
          ..write('***');
        start = right;
      }
      at = text.indexOf('@', at + 1);
    }
    out.write(text.substring(start));
    return out.toString();
  }

  /// true when [text] has a letter, digit or `_` at [index]. Every
  /// letter that is not ASCII counts as a letter.
  static bool _isWord(String text, int index) {
    if (index < 0 || index >= text.length) return false;
    int c = text.codeUnitAt(index);
    return (c >= 0x30 && c <= 0x39) ||
        (c >= 0x41 && c <= 0x5a) ||
        (c >= 0x61 && c <= 0x7a) ||
        c == 0x5f ||
        c >= 0x80;
  }

  /// every text of [mail] (from `Message.toJson()`), with the names of
  /// the headers and the keys of the metadata, but not the tags and the
  /// file bytes and content types
  static void _collect(Map<String, Object?> mail, List<String> into) {
    for (MapEntry<String, Object?> entry in mail.entries) {
      Object? value = entry.value;
      switch (entry.key) {
        case 'tags':
          break;
        case 'attachments':
          for (Object? file in value is List ? value : const <Object?>[]) {
            if (file is! Map) continue;
            for (MapEntry<Object?, Object?> field in file.entries) {
              if (field.key == 'bytes' || field.key == 'contentType') continue;
              _add(field.value, into);
            }
          }
        case 'headers' || 'metadata':
          if (value is Map) _add(value.keys.toList(), into);
          _add(value, into);
        default:
          _add(value, into);
      }
    }
  }

  /// every text in [value], with the values of a map, not its keys
  static void _add(Object? value, List<String> into) {
    if (value is String) {
      into.add(value);
    } else if (value is Map) {
      for (Object? item in value.values) {
        _add(item, into);
      }
    } else if (value is List) {
      for (Object? item in value) {
        _add(item, into);
      }
    }
  }

  @override
  String toString() {
    String code = statusCode == null ? '' : ' (code $statusCode)';
    return 'RedactedMailError$code: $message';
  }
}

/// The texts of a mail that start with the same 3 letters, by length.
class _Texts {
  final Map<int, Set<String>> _byLength = <int, Set<String>>{};
  final Map<int, Set<int>> _hashes = <int, Set<int>>{};
  List<int> _lengths = <int>[];

  void add(String text) {
    Set<String>? same = _byLength[text.length];
    if (same == null) {
      same = <String>{};
      _byLength[text.length] = same;
      _hashes[text.length] = <int>{};
      _lengths = <int>[..._lengths, text.length]..sort(_down);
    }
    same.add(text);
    _hashes[text.length]!.add(_Hash(text).of(0, text.length));
  }

  static int _down(int a, int b) => b - a;

  /// the length of the longest text that is a whole word in [lower] at
  /// [at], or null. The [hash] of [lower] skips most places without
  /// copying the text, so many texts of the same start stay fast.
  int? longestAt(String lower, _Hash hash, int at) {
    for (int length in _lengths) {
      int end = at + length;
      if (end > lower.length) continue;
      if (!_hashes[length]!.contains(hash.of(at, end))) continue;
      String part = lower.substring(at, end);
      if (!_byLength[length]!.contains(part)) continue;
      if (RedactedMailError._isWord(part, 0) &&
          RedactedMailError._isWord(lower, at - 1)) {
        continue;
      }
      if (RedactedMailError._isWord(part, length - 1) &&
          RedactedMailError._isWord(lower, end)) {
        continue;
      }
      return length;
    }
    return null;
  }
}

/// A rolling hash of a text: the hash of any part in constant time.
class _Hash {
  _Hash(String text)
    : _prefix = List<int>.filled(text.length + 1, 0),
      _power = List<int>.filled(text.length + 1, 1) {
    for (int i = 0; i < text.length; i++) {
      _prefix[i + 1] = (_prefix[i] * _base + text.codeUnitAt(i)) % _mod;
      _power[i + 1] = _power[i] * _base % _mod;
    }
  }

  static const int _mod = 2147483647;
  static const int _base = 131;

  final List<int> _prefix;
  final List<int> _power;

  /// the hash of the part from [start] to [end]
  int of(int start, int end) {
    int value = (_prefix[end] - _prefix[start] * _power[end - start]) % _mod;
    return value < 0 ? value + _mod : value;
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
