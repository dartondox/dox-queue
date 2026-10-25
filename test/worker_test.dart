import 'dart:async';
import 'dart:convert';

import 'package:dox_queue/dox_queue.dart';
import 'package:test/test.dart';

import 'fixtures/jobs.g.dart';
import 'fixtures/test_jobs.dart';
import 'support.dart';

Future<void> _wait(int millis) {
  return Future<void>.delayed(Duration(milliseconds: millis));
}

void main() {
  setUpAll(setUpTestApp);

  for (TestStore store in testStores) {
    group('${store.name} worker', () {
      setUp(() async {
        await store.reset();
        JobLog.clear();
        Queue.configure(
          store.config(
            jobs: jobRegistry,
            retryAfter: const Duration(milliseconds: 500),
          ),
        );
      });

      tearDown(Queue.close);

      /// work the queues until they are empty
      Future<void> work({
        List<String> queues = const <String>['default', 'slow'],
        int concurrency = 1,
      }) async {
        await Worker(
          driver: Queue.driver,
          jobs: jobRegistry,
          options: WorkerOptions(
            queues: queues,
            concurrency: concurrency,
            stopWhenEmpty: true,
          ),
        ).run();
      }

      test('runs a job and deletes it', () async {
        String id = await SendWelcomeEmail(userId: 7).dispatch();
        expect(id, hasLength(32));
        await work();
        expect(JobLog.handled, <String>['welcome 7']);
        expect(await Queue.driver.pop('default'), isNull);
        expect(await Queue.driver.failed(), isEmpty);
      });

      test('retries with backoff, then works', () async {
        await FlakyJob(key: 'a', failTimes: 2).dispatch();
        Stopwatch watch = Stopwatch()..start();
        // the backoff makes the queue look empty for a moment
        while (JobLog.handled.isEmpty && watch.elapsed.inSeconds < 5) {
          await work();
          await _wait(20);
        }
        expect(JobLog.runs['a'], 3);
        expect(JobLog.handled, <String>['flaky a']);
        // two waits of 150ms
        expect(watch.elapsedMilliseconds, greaterThanOrEqualTo(300));
        expect(JobLog.failed, isEmpty);
        expect(await Queue.driver.failed(), isEmpty);
      });

      test('fails after the last try and calls failed()', () async {
        await FlakyJob(key: 'b', failTimes: 9, maxTries: 2).dispatch();
        Stopwatch watch = Stopwatch()..start();
        while (JobLog.failed.isEmpty && watch.elapsed.inSeconds < 5) {
          await work();
          await _wait(20);
        }
        expect(JobLog.runs['b'], 2);
        expect(JobLog.failed, hasLength(1));
        expect(JobLog.failed.first, contains('flaky b run 2'));
        List<FailedJob> failed = await Queue.driver.failed();
        expect(failed, hasLength(1));
        expect(failed.first.job, 'FlakyJob');
        expect(failed.first.error, contains('flaky b run 2'));
      });

      test('a job that runs too long fails with a timeout', () async {
        await SlowJob(key: 't', millis: 400, timeoutMillis: 50).dispatch();
        await work();
        expect(JobLog.failed, <String>['slow t: TimeoutException']);
        List<FailedJob> failed = await Queue.driver.failed();
        expect(failed.single.error, contains('timed out'));
        // handle() keeps running in the background: let it end
        await _wait(400);
      });

      test('a delayed job runs after its delay', () async {
        await SendWelcomeEmail(userId: 1)
            .dispatch(delay: const Duration(milliseconds: 300));
        await work();
        expect(JobLog.handled, isEmpty);
        await _wait(400);
        await work();
        expect(JobLog.handled, <String>['welcome 1']);
      });

      test('dispatch(queue:) wins over job.queue', () async {
        await SendWelcomeEmail(userId: 2).dispatch(queue: 'emails');
        await work();
        expect(JobLog.handled, isEmpty);
        await work(queues: <String>['emails']);
        expect(JobLog.handled, <String>['welcome 2']);
      });

      test('runs the job of a crashed worker again', () async {
        await SendWelcomeEmail(userId: 3).dispatch();
        // a worker takes the job and dies
        expect(await Queue.driver.pop('default'), isNotNull);
        await work();
        expect(JobLog.handled, isEmpty);
        await _wait(600);
        // tries is 1 and the job was taken once: it fails, no second run
        await work();
        expect(JobLog.handled, isEmpty);
        List<FailedJob> failed = await Queue.driver.failed();
        expect(failed.single.error, contains('did not finish'));

        // with more tries it runs again
        await FlakyJob(key: 'c', failTimes: 0).dispatch();
        expect(await Queue.driver.pop('default'), isNotNull);
        await _wait(600);
        await work();
        expect(JobLog.handled, <String>['flaky c']);
      });

      test('runs jobs at once with concurrency', () async {
        for (int i = 0; i < 4; i++) {
          await SlowJob(key: '$i', millis: 300).dispatch();
        }
        Stopwatch watch = Stopwatch()..start();
        await work(concurrency: 4);
        expect(JobLog.handled, hasLength(4));
        expect(watch.elapsedMilliseconds, lessThan(1100));
      });

      test('stop() lets the running job finish', () async {
        await SlowJob(key: 's', millis: 300).dispatch();
        Worker worker = Worker(
          driver: Queue.driver,
          jobs: jobRegistry,
          options: const WorkerOptions(
            queues: <String>['slow'],
            sleep: Duration(seconds: 30),
          ),
        );
        Future<void> running = worker.run();
        await _wait(100);
        worker.stop();
        await running.timeout(const Duration(seconds: 3));
        expect(JobLog.handled, <String>['slow s']);
        expect(await Queue.driver.pop('slow'), isNull);
        expect(await Queue.driver.failed(), isEmpty);
      });

      test('stop() ends a sleep at once', () async {
        Worker worker = Worker(
          driver: Queue.driver,
          jobs: jobRegistry,
          options: const WorkerOptions(sleep: Duration(seconds: 30)),
        );
        Future<void> running = worker.run();
        await _wait(100);
        worker.stop();
        await running.timeout(const Duration(seconds: 2));
      });

      test('a job with an unknown name fails at once', () async {
        await Queue.driver.push(
          JobPayload(id: 'x', job: 'GoneJob', queue: 'default', data: '{}'),
        );
        await work();
        List<FailedJob> failed = await Queue.driver.failed();
        expect(failed.single.error, contains('No job class named GoneJob'));
      });

      test('saves and loads every field type', () async {
        DateTime at = DateTime.utc(2026, 10, 25, 8, 30);
        AllTypesJob job = AllTypesJob(
          3,
          9.5,
          2,
          'hello',
          true,
          at,
          Color.green,
          note: 'n',
          tags: <String>['a', 'b'],
          scores: <String, double>{'x': 1.0},
          dates: <DateTime?>[at, null],
          nested: <List<int>>[
            <int>[1, 2],
            <int>[3],
          ],
          colors: <String, Color>{'c': Color.red},
          extra: <String, dynamic>{
            'k': <int>[1],
          },
          maybeAt: at,
        );
        await Queue.dispatch(job);
        ReservedJob reserved = (await Queue.driver.pop('default'))!;
        JobCodec codec = jobRegistry.byName(reserved.payload.job)!;
        expect(codec.name, 'all-types');
        // the worker reads it the same way
        AllTypesJob loaded = codec.decode(
          jsonDecode(reserved.payload.data) as Map<String, Object?>,
        ) as AllTypesJob;
        expect(loaded.count, 3);
        expect(loaded.price, 9.5);
        expect(loaded.amount, 2);
        expect(loaded.title, 'hello');
        expect(loaded.active, isTrue);
        expect(loaded.at, at);
        expect(loaded.color, Color.green);
        expect(loaded.note, 'n');
        expect(loaded.tags, <String>['a', 'b']);
        expect(loaded.scores, <String, double>{'x': 1.0});
        expect(loaded.dates, <DateTime?>[at, null]);
        expect(loaded.nested, <List<int>>[
          <int>[1, 2],
          <int>[3],
        ]);
        expect(loaded.colors, <String, Color>{'c': Color.red});
        expect(loaded.extra, <String, dynamic>{
          'k': <int>[1],
        });
        expect(loaded.maybeAt, at);
        expect(loaded.maybeColor, isNull);
      });
    }, skip: store.skip);
  }
}
