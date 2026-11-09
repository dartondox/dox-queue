import 'dart:async';

import 'package:dox_core/dox_core.dart' show AppConfig, Dox, LoggerConfig;
import 'package:dox_mailer/dox_mailer.dart';
import 'package:dox_queue/dox_queue.dart';
import 'package:test/test.dart';

import 'support.dart';

late ArrayTransport mainMailer;
late ArrayTransport otherMailer;

/// a mail server that is down
class BrokenTransport extends Transport {
  int calls = 0;

  @override
  Future<SentMessage> send(Message message) async {
    calls++;
    throw MailException('server is down');
  }
}

late BrokenTransport brokenMailer;

/// a mail server that says no, and names the recipient and the subject,
/// like many SMTP servers do
class RejectTransport extends Transport {
  @override
  Future<SentMessage> send(Message message) async {
    throw MailException(
      '550 5.1.1 <ANN@example.com>: mailbox unavailable '
      '(subject "Your invoice", for Ann, forwarded to ann.lee@other.org)',
      statusCode: 550,
    );
  }
}

/// a mail server that quotes the whole body in its answer
class QuoteTransport extends Transport {
  @override
  Future<SentMessage> send(Message message) async {
    throw MailException(
      '552 message refused: ${message.html}',
      statusCode: 552,
    );
  }
}

/// every line the worker logs while [body] runs
Future<String> _logOf(Future<void> Function() body) async {
  StringBuffer log = StringBuffer();
  void keep(Zone self, ZoneDelegate parent, Zone zone, String line) {
    log.writeln(line);
  }

  await runZoned(body, zoneSpecification: ZoneSpecification(print: keep));
  return log.toString();
}

/// the parts of the test mail that must not be in a log
const List<String> _secrets = <String>[
  'ann@example.com',
  'ann.lee@other.org',
  'Your invoice',
  'Ann',
];

Message _message() => Message(
  to: <Address>[const Address('ann@example.com', 'Ann')],
  bcc: <Address>[const Address('boss@example.com')],
  subject: 'Your invoice',
  text: 'Hi Ann',
  html: '<p>Hi Ann</p>',
  attachments: <Attachment>[
    Attachment(<int>[0, 1, 2, 255], 'invoice.pdf'),
  ],
);

/// take one job of [queue] and run it with an app registry that has no
/// mail job, like a real app
Future<JobPayload> _work([String queue = 'default', int tries = 1]) async {
  ReservedJob? job = await Queue.driver.pop(queue);
  expect(job, isNotNull, reason: 'no job on $queue');
  await Worker(
    driver: Queue.driver,
    jobs: const JobRegistry(<JobCodec>[]),
    options: WorkerOptions(tries: tries),
  ).process(job!);
  return job.payload;
}

void main() {
  setUpAll(() {
    setUpTestApp();
    // log on, so the tests can read what the worker logs
    Dox().initialize(
      AppConfig(
        appKey: 'k' * 32,
        totalIsolate: 1,
        logger: LoggerConfig(enabled: true),
      ),
    );
  });

  setUp(() {
    Mail.restore();
    mainMailer = ArrayTransport();
    otherMailer = ArrayTransport();
    brokenMailer = BrokenTransport();
    Mail.configure(
      MailConfig(
        defaultMailer: 'main',
        from: const Address('app@example.com', 'App'),
        mailers: <String, Transport>{
          'main': mainMailer,
          'other': otherMailer,
          'broken': brokenMailer,
          'reject': RejectTransport(),
          'quote': QuoteTransport(),
        },
      ),
    );
  });

  tearDown(Queue.close);

  test('with the sync driver, Mail.queue sends at once', () async {
    Queue.configure(QueueConfig());
    await Mail.mailer('other').queue(_message());
    expect(otherMailer.messages.single.subject, 'Your invoice');
    expect(mainMailer.sent, isEmpty);
  });

  for (TestStore store in testStores) {
    group(store.name, skip: store.skip, () {
      setUp(() async {
        await store.reset();
        Queue.configure(store.config());
      });

      test('Mail.queue saves the mail, the worker sends it', () async {
        String id = await Mail.queue(_message());
        expect(mainMailer.sent, isEmpty);
        JobPayload payload = await _work();
        expect(payload.id, id);
        expect(payload.job, 'dox_queue:SendQueuedMail');
        Message sent = mainMailer.messages.single;
        expect(sent.from, const Address('app@example.com', 'App'));
        expect(sent.to, <Address>[const Address('ann@example.com', 'Ann')]);
        expect(sent.bcc, <Address>[const Address('boss@example.com')]);
        expect(sent.html, '<p>Hi Ann</p>');
        expect(sent.attachments.single.bytes, <int>[0, 1, 2, 255]);
        expect(sent.attachments.single.contentType, 'application/pdf');
        expect(await Queue.driver.failed(), isEmpty);
      });

      test('the worker sends with the same mailer', () async {
        await Mail.mailer('other').queue(_message(), queue: 'emails');
        await _work('emails');
        expect(otherMailer.sent, hasLength(1));
        expect(mainMailer.sent, isEmpty);
      });

      test('Mail.later waits for the delay', () async {
        await Mail.later(const Duration(minutes: 5), _message());
        expect(await Queue.driver.pop('default'), isNull);
        expect(mainMailer.sent, isEmpty);
      });

      test('a failing transport is retried, then the job fails', () async {
        await Mail.mailer('broken').queue(_message());
        await _work('default', 2);
        expect(brokenMailer.calls, 1);
        expect(await Queue.driver.failed(), isEmpty);
        await _work('default', 2);
        expect(brokenMailer.calls, 2);
        List<FailedJob> failed = await Queue.driver.failed();
        expect(failed.single.job, 'dox_queue:SendQueuedMail');
        expect(failed.single.error, contains('server is down'));
        expect(await Queue.driver.pop('default'), isNull);
      });

      test('encrypt: true: a send error shows no mail data', () async {
        await Mail.mailer('reject').queue(_message(), encrypt: true);
        String log = await _logOf(_retryThenFail);
        List<FailedJob> failed = await Queue.driver.failed();
        String error = failed.single.error;
        // the encrypted details are random base64 text, and a short
        // secret like "Ann" can be in it by chance. Check the text
        // around them only.
        String plainLog = log.replaceAll(_encrypted, '');
        String plainError = error.replaceAll(_encrypted, '');
        for (String secret in _secrets) {
          expect(plainLog, isNot(contains(secret)));
          expect(plainError, isNot(contains(secret)));
        }
        // still enough to find the problem
        printOnFailure(log);
        expect(log, contains('failed try 1 of 2'));
        expect(log, contains('RedactedMailError: MailException (code 550).'));
        expect(error, contains('RedactedMailError: MailException (code 550).'));
        // the app key gives back the full error
        expect(
          RedactedMailError.decrypt(error),
          startsWith('MailException: 550 5.1.1 <ANN@example.com>'),
        );
      });

      test('encrypt: true: a 100 KB body is not in the error', () async {
        String html = '<p>Your reset code is 918273</p>${'x' * 100 * 1024}';
        Message message = _message().copyWith(html: html);
        await Mail.mailer('quote').queue(message, encrypt: true);
        String log = await _logOf(_retryThenFail);
        String error = (await Queue.driver.failed()).single.error;
        for (String text in <String>[log, error]) {
          // the encrypted details are random base64 text. About 1 in 300
          // has "xxxx" in it by chance, and each run makes several. Check
          // the text around them only.
          text = text.replaceAll(_encrypted, 'Details are encrypted: ...');
          expect(text, isNot(contains('918273')));
          expect(text, isNot(contains('xxxx')));
          expect(text, contains('MailException (code 552). Details are'));
        }
      });

      test('a plain mail job keeps the full error, as before', () async {
        await Mail.mailer('reject').queue(_message());
        await _logOf(_retryThenFail);
        List<FailedJob> failed = await Queue.driver.failed();
        expect(failed.single.error, contains('<ANN@example.com>'));
      });

      test('encrypt: true saves the mail encrypted', () async {
        await Mail.queue(_message(), encrypt: true);
        JobPayload payload = await _work();
        expect(payload.job, 'dox_queue:SendEncryptedQueuedMail');
        expect(payload.data, isNot(contains('ann@example.com')));
        expect(payload.data, isNot(contains('Your invoice')));
        expect(payload.data, isNot(contains('Hi Ann')));
        expect(mainMailer.messages.single.subject, 'Your invoice');
      });
    });
  }
}

/// the encrypted details of a [RedactedMailError] text
final RegExp _encrypted = RegExp(r'Details are encrypted: [^\s"]+');

/// two tries: the first is released, the second fails
Future<void> _retryThenFail() async {
  await _work('default', 2);
  await _work('default', 2);
}
