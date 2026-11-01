import 'dart:io';

import 'package:dox_mailer/dox_mailer.dart';
import 'package:dox_queue/src/send_queued_mail.dart';
import 'package:test/test.dart';

/// an error whose text can not be made, so the redaction fails
class BadTextError extends MailException {
  BadTextError() : super('ann@example.com', statusCode: 421);

  @override
  String toString() => throw StateError('no text');
}

Map<String, Object?> _mail({String body = 'Hi Ann', List<String>? tags}) {
  return Message(
    from: const Address('app@example.com', 'App'),
    to: <Address>[const Address('ann@example.com', 'Ann')],
    subject: 'Your reset code',
    text: body,
    tags: tags ?? <String>[],
  ).toJson();
}

/// a body of about [size] letters with a secret code in it
String _bigBody(int size) {
  return 'Hi Ann, your reset code is 918273. ${'x' * size}';
}

void main() {
  for (int size in <int>[100 * 1024, 1024 * 1024]) {
    test('a ${size ~/ 1024} KB body is hidden, type and code stay', () {
      String body = _bigBody(size);
      RedactedMailError error = RedactedMailError.of(
        MailException('552 message refused: $body', statusCode: 552),
        _mail(body: body),
      );
      String text = '$error';
      expect(text, isNot(contains('918273')));
      expect(text, isNot(contains('xxxx')));
      expect(
        text,
        'RedactedMailError (code 552): '
        'MailException: 552 message refused: ***',
      );
      expect(error.type, 'MailException');
    });
  }

  test('a big body that is not in the error does not break it', () {
    RedactedMailError error = RedactedMailError.of(
      MailException('550 <ann@example.com>: no mailbox', statusCode: 550),
      _mail(body: _bigBody(1024 * 1024)),
    );
    expect(
      '$error',
      'RedactedMailError (code 550): '
          'MailException: 550 <***>: no mailbox',
    );
  });

  test('when the redaction fails, only the type and code are left', () {
    RedactedMailError error = RedactedMailError.of(BadTextError(), _mail());
    expect(error.type, 'BadTextError');
    expect(error.statusCode, 421);
    expect(
      '$error',
      'RedactedMailError (code 421): '
          'BadTextError: (could not redact)',
    );
  });

  test('a tag or a name does not hide other words', () {
    RedactedMailError error = RedactedMailError.of(
      MailException('SMTP: Annual limit, Ann can not get more'),
      _mail(tags: <String>['smtp']),
    );
    expect(
      '$error',
      'RedactedMailError: '
          'MailException: SMTP: Annual limit, *** can not get more',
    );
  });

  test('a SocketException keeps its host and port, no code', () {
    RedactedMailError error = RedactedMailError.of(
      SocketException(
        'Connection refused',
        address: InternetAddress('10.0.0.5'),
        port: 587,
      ),
      _mail(),
    );
    expect(error.type, 'SocketException');
    expect(error.statusCode, isNull);
    expect('$error', contains('Connection refused'));
    expect('$error', contains('10.0.0.5'));
    expect('$error', contains('587'));
  });
}
