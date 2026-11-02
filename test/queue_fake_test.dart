import 'package:dox_mailer/dox_mailer.dart';
import 'package:dox_queue/dox_queue.dart';
import 'package:test/test.dart';

import 'fixtures/test_jobs.dart';

/// the text of the AssertionError that [body] throws
String _failure(void Function() body) {
  try {
    body();
  } on AssertionError catch (error) {
    return '${error.message}';
  }
  fail('it did not throw');
}

bool _isUser2(SendWelcomeEmail job) => job.userId == 2;

bool _isUser9(SendWelcomeEmail job) => job.userId == 9;

void main() {
  late QueueFake queue;

  setUp(() {
    JobLog.clear();
    queue = Queue.fake();
  });

  tearDown(Queue.restore);

  test('keeps the job, with its queue and delay, and runs nothing', () async {
    String id = await SendWelcomeEmail(userId: 1)
        .dispatch(delay: Duration(minutes: 5), queue: 'emails');
    await Queue.dispatch(SendWelcomeEmail(userId: 2));

    expect(id, hasLength(32));
    expect(JobLog.handled, isEmpty);
    expect(queue.pushed, hasLength(2));
    expect(queue.pushed.first.queue, 'emails');
    expect(queue.pushed.first.delay, Duration(minutes: 5));
    expect(queue.pushed.last.queue, 'default');
    expect(queue.pushed.last.delay, isNull);
    expect(queue.jobsOf<SendWelcomeEmail>().last.userId, 2);
  });

  test('dispatchSync still runs the job', () async {
    await SendWelcomeEmail(userId: 3).dispatchSync();
    expect(JobLog.handled, <String>['welcome 3']);
    queue.assertNothingPushed();
  });

  test('assertPushed', () async {
    expect(
      _failure(queue.assertPushed<SendWelcomeEmail>),
      'Expected a SendWelcomeEmail job to be pushed. No jobs were pushed.',
    );
    await SendWelcomeEmail(userId: 2).dispatch();
    queue.assertPushed<SendWelcomeEmail>();
    queue.assertPushed<SendWelcomeEmail>(_isUser2);
    expect(
      _failure(() => queue.assertPushed<SendWelcomeEmail>(_isUser9)),
      'Expected a SendWelcomeEmail job that passes the test to be pushed, '
      'but none of the 1 pushed SendWelcomeEmail jobs passed it.',
    );
    expect(
      _failure(queue.assertPushed<AllTypesJob>),
      'Expected a AllTypesJob job to be pushed. Pushed: SendWelcomeEmail.',
    );
  });

  test('assertPushedTimes', () async {
    await SendWelcomeEmail(userId: 1).dispatch();
    await SendWelcomeEmail(userId: 2).dispatch();
    queue.assertPushedTimes<SendWelcomeEmail>(2);
    queue.assertPushedTimes<AllTypesJob>(0);
    expect(
      _failure(() => queue.assertPushedTimes<SendWelcomeEmail>(1)),
      'Expected SendWelcomeEmail to be pushed 1 times, but it was pushed '
      '2 times.',
    );
  });

  test('assertNotPushed', () async {
    queue.assertNotPushed<SendWelcomeEmail>();
    await SendWelcomeEmail(userId: 1).dispatch();
    expect(
      _failure(queue.assertNotPushed<SendWelcomeEmail>),
      'Expected no SendWelcomeEmail job to be pushed, but it was pushed '
      '1 times.',
    );
  });

  test('assertNothingPushed', () async {
    queue.assertNothingPushed();
    await SendWelcomeEmail(userId: 1).dispatch();
    expect(
      _failure(queue.assertNothingPushed),
      'Expected no jobs to be pushed. Pushed: SendWelcomeEmail.',
    );
    queue.clear();
    queue.assertNothingPushed();
  });

  test('catches Mail.queue() and Mail.later()', () async {
    ArrayTransport mail = ArrayTransport();
    Mail.configure(
      MailConfig(
        defaultMailer: 'main',
        mailers: <String, Transport>{'main': mail},
      ),
    );
    Message message = Message(
      from: Address('app@example.com'),
      to: <Address>[Address('ann@example.com')],
      subject: 'Hi',
      text: 'Hello',
    );
    await Mail.queue(message);
    await Mail.later(Duration(minutes: 1), message, queue: 'mail');

    expect(mail.sent, isEmpty);
    expect(queue.pushed, hasLength(2));
    expect(queue.pushed.last.queue, 'mail');
    expect(queue.pushed.last.delay, Duration(minutes: 1));
  });

  test('restore takes back Mail.queue() when there is no config', () {
    Queue.restore();
    // like before Queue.fake(): Mail.queue() has no queue
    expect(() => Mail.mailQueue, throwsStateError);
  });

  test('restore stops the fake', () async {
    Queue.restore();
    Queue.configure(QueueConfig());
    await SendWelcomeEmail(userId: 4).dispatch();
    expect(JobLog.handled, <String>['welcome 4']);
    expect(queue.pushed, isEmpty);
  });
}
