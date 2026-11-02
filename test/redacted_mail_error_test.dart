import 'dart:io';

import 'package:dox_core/dox_core.dart' show AppConfig, DecryptException, Dox;
import 'package:dox_mailer/dox_mailer.dart';
import 'package:dox_queue/dox_queue.dart';
import 'package:test/test.dart';

/// an error whose text can not be made, so it can not be encrypted
class BadTextError extends MailException {
  BadTextError() : super('ann@example.com', statusCode: 421);

  @override
  String toString() => throw StateError('no text');
}

/// an error with a given text
class TextError implements Exception {
  TextError(this.text);

  final String text;

  @override
  String toString() => text;
}

/// an error whose type can not be read
class BadTypeError implements Exception {
  @override
  Type get runtimeType => throw StateError('ann@example.com');

  @override
  String toString() => 'ann@example.com';
}

/// the texts that must never be in the plain text of the error
const List<String> _secrets = <String>[
  'ann@example.com',
  'ANN@EXAMPLE.COM',
  'Ann Lee',
  'Zoë',
  '918273',
  'xxxx',
  'LLLL',
  'other.org',
];

/// Many kinds of error text that leaked with text redaction: names in
/// curly quotes, texts that overlap, a 1 MB body, a long local part.
List<String> _errors = <String>[
  '550 5.1.1 <ANN@EXAMPLE.COM>: mailbox unavailable',
  'rejected for “Ann Lee” and ‘Zoë’',
  'Ann Leeann@example.com overlaps Ann Lee',
  '552 refused: Hi Ann, your code is 918273. ${'x' * 1024 * 1024}',
  '550 <${'L' * 200}@other.org>: no mailbox',
];

/// the plain text part: the error without its encrypted details
String _plain(String error) => error.split('Details are encrypted: ').first;

void main() {
  setUpAll(() {
    Dox().initialize(AppConfig(appKey: 'k' * 32, totalIsolate: 1));
  });

  for (String text in _errors) {
    test('no mail data in: ${text.substring(0, 30)}', () {
      String error =
          '${RedactedMailError.of(MailException(text, statusCode: 550))}';
      for (String secret in _secrets) {
        expect(_plain(error), isNot(contains(secret)));
        expect(error, isNot(contains(secret)));
      }
      expect(
        error,
        startsWith('RedactedMailError: MailException (code 550). '),
      );
    });
  }

  test('decrypt gives back the full error text', () {
    String text = '550 5.1.1 <ann@example.com>: mailbox unavailable';
    String error =
        '${RedactedMailError.of(MailException(text, statusCode: 550))}';
    // like a failed job: the error, then the stack trace
    String saved = '$error\n#0 main (file:///app.dart:1:1)';
    expect(RedactedMailError.decrypt(saved), 'MailException: $text');
  });

  test('only the first 64K characters of a huge error are encrypted', () {
    String text = 'x' * (5 * 1024 * 1024);
    RedactedMailError error = RedactedMailError.of(MailException(text));
    expect('$error'.length, lessThan(100 * 1024));
    expect(
      RedactedMailError.decrypt('$error'),
      hasLength(RedactedMailError.maxDetails),
    );
  });

  test('a wrong key can not read the details', () {
    String error = '${RedactedMailError.of(MailException('ann@example.com'))}';
    Dox().initialize(AppConfig(appKey: 'z' * 32, totalIsolate: 1));
    addTearDown(_resetKey);
    expect(
      () => RedactedMailError.decrypt(error),
      throwsA(isA<DecryptException>()),
    );
  });

  test('an error with no details gives null', () {
    expect(RedactedMailError.decrypt('StateError: boom'), isNull);
  });

  test('when the text can not be made, only type and code stay', () {
    RedactedMailError error = RedactedMailError.of(BadTextError());
    expect(error.type, 'BadTextError');
    expect(error.statusCode, 421);
    expect(error.details, isNull);
    expect(
      '$error',
      'RedactedMailError: BadTextError (code 421). '
          'details could not be encrypted',
    );
  });

  test('a SocketException keeps its type, with no code', () {
    RedactedMailError error = RedactedMailError.of(
      SocketException(
        'Connection refused',
        address: InternetAddress('10.0.0.5'),
        port: 587,
      ),
    );
    expect(error.type, 'SocketException');
    expect(error.statusCode, isNull);
    expect('$error', startsWith('RedactedMailError: SocketException. '));
    expect(RedactedMailError.decrypt('$error'), contains('10.0.0.5'));
  });

  test('the cut does not split an emoji', () {
    int max = RedactedMailError.maxDetails;
    String text = '${'x' * (max - 1)}😀 and more';
    String? back = RedactedMailError.decrypt(
      '${RedactedMailError.of(TextError(text))}',
    );
    expect(back, 'x' * (max - 1));
  });

  test('the answer of the server is in the encrypted details', () {
    RedactedMailError error = RedactedMailError.of(
      MailException(
        'the API said no',
        statusCode: 422,
        body: '{"to":"ann@example.com"}',
      ),
    );
    expect('$error', isNot(contains('ann@example.com')));
    expect(
      RedactedMailError.decrypt('$error'),
      'MailException: the API said no\nbody: {"to":"ann@example.com"}',
    );
  });

  test('an error whose type throws gives only "unknown"', () {
    RedactedMailError error = RedactedMailError.of(BadTypeError());
    expect(
      '$error',
      'RedactedMailError: unknown. details could not be encrypted',
    );
  });

  test('decrypt only takes real encrypted details', () {
    expect(
      () => RedactedMailError.decrypt(
        'RedactedMailError: X. Details are encrypted: {"to":"ann@example.com"}',
      ),
      throwsA(isA<DecryptException>()),
    );
  });
}

void _resetKey() {
  Dox().initialize(AppConfig(appKey: 'k' * 32, totalIsolate: 1));
}
