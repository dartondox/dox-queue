import 'package:dox_mailer/dox_mailer.dart';
import 'package:dox_queue/dox_queue.dart';
import 'package:test/test.dart';

import 'support.dart';

late ArrayTransport mainMailer;
late ArrayTransport otherMailer;

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
Future<JobPayload> _work([String queue = 'default']) async {
  ReservedJob? job = await Queue.driver.pop(queue);
  expect(job, isNotNull, reason: 'no job on $queue');
  await Worker(
    driver: Queue.driver,
    jobs: const JobRegistry(<JobCodec>[]),
  ).process(job!);
  return job.payload;
}

void main() {
  setUpAll(setUpTestApp);

  setUp(() {
    Mail.restore();
    mainMailer = ArrayTransport();
    otherMailer = ArrayTransport();
    Mail.configure(
      MailConfig(
        defaultMailer: 'main',
        from: const Address('app@example.com', 'App'),
        mailers: <String, Transport>{'main': mainMailer, 'other': otherMailer},
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

      test('encrypt: true saves the mail encrypted', () async {
        await Mail.queue(_message(), encrypt: true);
        JobPayload payload = await _work();
        expect(payload.job, 'dox_queue:SendEncryptedQueuedMail');
        expect(payload.data, isNot(contains('ann@example.com')));
        expect(mainMailer.messages.single.subject, 'Your invoice');
      });
    });
  }
}
