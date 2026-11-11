import 'package:dox_core/dox_core.dart' show AppConfig, Dox, LoggerConfig;
import 'package:dox_queue/dox_queue.dart';
import 'package:test/test.dart';

import 'fixtures/jobs.g.dart';
import 'fixtures/test_jobs.dart';
import 'support.dart';

/// the events, as `<type> <job name>`
List<String> events = <String>[];

void log(QueueEvent event) {
  events.add('${event.type.name} ${event.name ?? '-'}');
}

void main() {
  setUpAll(setUpTestApp);
  setUp(() {
    JobLog.clear();
    events.clear();
    for (QueueEventType type in QueueEventType.values) {
      QueueEvent.listen(type, log);
    }
  });
  tearDown(Queue.forgetListeners);

  for (TestStore store in testStores) {
    group('${store.name} queue events', () {
      setUp(() async {
        await store.reset();
        Queue.configure(store.config(jobs: jobRegistry));
      });

      tearDown(Queue.close);

      Future<void> work() {
        return Worker(
          driver: Queue.driver,
          jobs: jobRegistry,
          options: const WorkerOptions(stopWhenEmpty: true),
        ).run();
      }

      /// the events without `looping`
      List<String> jobEvents() {
        return <String>[
          for (String event in events)
            if (!event.startsWith('looping')) event,
        ];
      }

      test('a job that works: each event fires once', () async {
        await SendWelcomeEmail(userId: 1).dispatch();
        await work();
        expect(jobEvents(), <String>[
          'queued SendWelcomeEmail',
          'processing SendWelcomeEmail',
          'processed SendWelcomeEmail',
          'stopping -',
        ]);
        expect(events, contains('looping -'));
      });

      test('a retry fires exceptionOccurred and released', () async {
        QueueEvent? error;
        Queue.exceptionOccurred((QueueEvent event) => error = event);
        await FlakyJob(key: 'a', failTimes: 1).dispatch();
        await work();
        await Future<void>.delayed(const Duration(milliseconds: 200));
        await work();
        expect(jobEvents(), <String>[
          'queued FlakyJob',
          'processing FlakyJob',
          'exceptionOccurred FlakyJob',
          'released FlakyJob',
          'stopping -',
          'processing FlakyJob',
          'processed FlakyJob',
          'stopping -',
        ]);
        expect(error?.error, isA<StateError>());
        expect(error?.attempts, 1);
      });

      test('a job that fails for good fires failed once', () async {
        QueueEvent? failed;
        Queue.failing((QueueEvent event) => failed = event);
        await FlakyJob(key: 'b', failTimes: 5, maxTries: 1).dispatch();
        await work();
        expect(jobEvents(), <String>[
          'queued FlakyJob',
          'processing FlakyJob',
          'failed FlakyJob',
          'stopping -',
        ]);
        expect(failed?.job, isA<FlakyJob>());
        expect(failed?.error, isA<StateError>());
      });

      test('a throwing listener is logged, the worker goes on', () async {
        Queue.before((QueueEvent event) => throw StateError('bad listener'));
        await SendWelcomeEmail(userId: 1).dispatch();
        // log on, so the test can read the error
        Dox().initialize(
          AppConfig(
            appKey: 'k' * 32,
            totalIsolate: 1,
            logger: LoggerConfig(enabled: true),
          ),
        );
        String output;
        try {
          output = await logOf(work);
        } finally {
          Dox().initialize(AppConfig(appKey: 'k' * 32, totalIsolate: 1));
        }
        expect(output, contains('bad listener'));
        expect(JobLog.handled, <String>['welcome 1']);
        expect(jobEvents(), contains('processed SendWelcomeEmail'));
        expect(await Queue.driver.pop('default'), isNull);
      });
    }, skip: store.skip);
  }

  test('the sync driver and the fake fire no events', () async {
    Queue.configure(QueueConfig(jobs: jobRegistry));
    await SendWelcomeEmail(userId: 1).dispatch();
    Queue.fake();
    await SendWelcomeEmail(userId: 2).dispatch();
    Queue.restore();
    expect(events, isEmpty);
  });
}
