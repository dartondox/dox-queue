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
  setUpAll(setUpTestApp);

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
