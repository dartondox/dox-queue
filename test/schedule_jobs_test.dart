import 'package:dox_queue/dox_queue.dart';
import 'package:dox_scheduler/dox_scheduler.dart';
import 'package:test/test.dart';

import 'fixtures/test_jobs.dart';

void main() {
  late QueueFake fake;

  setUp(() => fake = Queue.fake());
  tearDown(Queue.restore);

  test('job() adds a task that dispatches the job', () async {
    Schedule schedule = Schedule();
    ScheduledTask task = schedule.job(SendWelcomeEmail(userId: 1));
    ScheduledTask named = schedule
        .job(SendWelcomeEmail(userId: 2), name: 'welcome', connection: 'db')
        .hourly();
    expect(task.name, 'SendWelcomeEmail');
    expect(named.name, 'welcome');
    fake.assertNothingPushed();
    await task.callback();
    await named.callback();
    expect(fake.pushed.map((PushedJob job) => job.connection), <String?>[
      null,
      'db',
    ]);
    fake.assertPushed<SendWelcomeEmail>(
      (SendWelcomeEmail job) => job.userId == 2,
    );
  });
}
