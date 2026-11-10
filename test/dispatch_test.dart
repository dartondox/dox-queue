import 'package:dox_core/dox_core.dart';
import 'package:dox_core/testing.dart';
import 'package:dox_queue/dox_queue.dart';
import 'package:test/test.dart';

import 'fixtures/jobs.g.dart';
import 'fixtures/test_jobs.dart';
import 'support.dart';

class WelcomeRouter extends Router {
  @override
  void register() {
    Route.get('/welcome', _welcome);
    Route.get('/after', _after);
  }

  /// the job must not run before the response is sent
  static Future<String> _after(DoxRequest req) async {
    SendWelcomeEmail(userId: 7).dispatchAfterResponse();
    await _wait(50);
    return JobLog.handled.join(',');
  }

  static Future<String> _welcome(DoxRequest req) {
    return SendWelcomeEmail(userId: 5).dispatch();
  }
}

TestStore _sqlite = testStores.first;

AppConfig _app(QueueConfig config) {
  return AppConfig(
    appKey: 'k' * 32,
    totalIsolate: 1,
    routers: <Router>[WelcomeRouter()],
    providers: <DoxProvider>[QueueProvider(config)],
  );
}

Future<void> _wait(int millis) {
  return Future<void>.delayed(Duration(milliseconds: millis));
}

void main() {
  setUpAll(setUpTestApp);

  setUp(() async {
    JobLog.clear();
    await _sqlite.reset();
  });

  group('sync driver', () {
    setUp(() {
      Queue.configure(QueueConfig(jobs: jobRegistry));
    });

    test('dispatch runs the job at once', () async {
      String id = await SendWelcomeEmail(userId: 1).dispatch();
      expect(id, hasLength(32));
      expect(JobLog.handled, <String>['welcome 1']);
    });

    test('a failing job calls failed() and throws', () async {
      await expectLater(
        FlakyJob(key: 'x', failTimes: 1).dispatch(),
        throwsStateError,
      );
      expect(JobLog.failed, hasLength(1));
    });

    test('dispatchSync runs the job with any driver', () async {
      Queue.configure(_sqlite.config(jobs: jobRegistry));
      await SendWelcomeEmail(userId: 2).dispatchSync();
      expect(JobLog.handled, <String>['welcome 2']);
      expect(await Queue.driver.pop('default'), isNull);
    });
  });

  test('dispatch needs the job in the registry', () async {
    Queue.configure(_sqlite.config());
    await expectLater(
      SendWelcomeEmail(userId: 1).dispatch(),
      throwsA(
        isA<StateError>().having(
          (StateError e) => e.message,
          'message',
          contains('not in the job registry'),
        ),
      ),
    );
  });

  test(
    'a Dox app dispatches from a route, the in-process worker runs it',
    () async {
      QueueConfig config = QueueConfig(
        driver: 'database',
        jobs: jobRegistry,
        connection: 'sqlite',
        workInProcess: true,
      );
      DoxTestClient client = await DoxTestClient.start(_app(config));
      try {
        TestResponse res = await client.get('/welcome');
        expect(res.status, 200);
        expect(res.body, hasLength(32));
        for (int i = 0; i < 50 && JobLog.handled.isEmpty; i++) {
          await _wait(100);
        }
        expect(JobLog.handled, <String>['welcome 5']);
      } finally {
        await client.close();
      }
    },
  );

  group('dispatchAfterResponse', () {
    tearDown(AfterResponse.disable);

    test('runs the job after the response is sent', () async {
      QueueConfig config = QueueConfig(jobs: jobRegistry);
      DoxTestClient client = await DoxTestClient.start(_app(config));
      try {
        TestResponse res = await client.get('/after');
        expect(res.status, 200);
        expect(res.body, isEmpty);
        for (int i = 0; i < 50 && JobLog.handled.isEmpty; i++) {
          await _wait(10);
        }
        expect(JobLog.handled, <String>['welcome 7']);
      } finally {
        await client.close();
      }
    });

    test('outside a request the job runs soon', () async {
      Queue.configure(QueueConfig(jobs: jobRegistry));
      SendWelcomeEmail(userId: 8).dispatchAfterResponse();
      expect(JobLog.handled, isEmpty);
      await _wait(10);
      expect(JobLog.handled, <String>['welcome 8']);
    });
  });

  group('QueueWorker', () {
    QueueConfig config = QueueConfig(
      driver: 'database',
      jobs: jobRegistry,
      connection: 'sqlite',
    );

    Future<int> run(List<String> args) {
      return QueueWorker(app: _app(config)).run(args);
    }

    test('work --stop-when-empty runs the jobs', () async {
      Queue.configure(config);
      await SendWelcomeEmail(userId: 1).dispatch();
      await SendWelcomeEmail(userId: 2).dispatch(queue: 'emails');
      expect(await run(<String>['work', '--stop-when-empty']), 0);
      expect(JobLog.handled, <String>['welcome 1']);
      Queue.configure(config);
      expect(
        await run(<String>['--queue=emails,default', '--stop-when-empty']),
        0,
      );
      expect(JobLog.handled, <String>['welcome 1', 'welcome 2']);
    });

    test('work --once runs one job', () async {
      Queue.configure(config);
      await SendWelcomeEmail(userId: 1).dispatch();
      await SendWelcomeEmail(userId: 2).dispatch();
      expect(await run(<String>['work', '--once']), 0);
      expect(JobLog.handled, <String>['welcome 1']);
    });

    test('--tries retries a job that does not set tries', () async {
      Queue.configure(config);
      await SlowJob(key: 'a', millis: 300, timeoutMillis: 10).dispatch();
      expect(
        await run(<String>[
          '--queue=slow',
          '--tries=2',
          '--stop-when-empty',
          '--sleep=0',
        ]),
        0,
      );
      expect(JobLog.failed, <String>['slow a: TimeoutException']);
      await _wait(400);
    });

    test('failed, retry, forget and flush', () async {
      Queue.configure(config);
      await FlakyJob(key: 'f', failTimes: 9, maxTries: 1).dispatch();
      await FlakyJob(key: 'g', failTimes: 9, maxTries: 1).dispatch();
      expect(await run(<String>['work', '--stop-when-empty']), 0);
      expect(JobLog.failed, hasLength(2));
      expect(await run(<String>['failed']), 0);

      Queue.configure(config);
      List<FailedJob> failed = await Queue.driver.failed();
      expect(failed, hasLength(2));
      expect(await run(<String>['retry', failed.first.id]), 0);
      expect(await run(<String>['retry', 'nope']), 1);
      Queue.configure(config);
      expect(await Queue.driver.failed(), hasLength(1));
      expect(await Queue.driver.pop('default'), isNotNull);

      expect(await run(<String>['retry', 'all']), 0);
      Queue.configure(config);
      expect(await Queue.driver.failed(), isEmpty);

      expect(await run(<String>['work', '--stop-when-empty']), 0);
      expect(await run(<String>['forget', 'nope']), 1);
      Queue.configure(config);
      String id = (await Queue.driver.failed()).first.id;
      expect(await run(<String>['forget', id]), 0);
      expect(await run(<String>['flush']), 0);
      Queue.configure(config);
      expect(await Queue.driver.failed(), isEmpty);
    });

    test('clear and prune-failed', () async {
      Queue.configure(config);
      await SendWelcomeEmail(userId: 1).dispatch();
      await SendWelcomeEmail(userId: 2).dispatch(queue: 'emails');
      await FlakyJob(
        key: 'p',
        failTimes: 9,
        maxTries: 1,
      ).dispatch(queue: 'flaky');
      expect(await run(<String>['work', '--queue=flaky', '--once']), 0);
      expect(await run(<String>['clear']), 0);
      Queue.configure(config);
      expect(await Queue.driver.pop('default'), isNull);
      expect(await Queue.driver.pop('emails'), isNotNull);
      expect(await run(<String>['clear', 'emails']), 0);
      Queue.configure(config);
      expect(await Queue.driver.pop('emails'), isNull);
      // the failed job is new: 1 hour keeps it, 0 hours deletes it
      expect(await run(<String>['prune-failed', '--hours=1']), 0);
      Queue.configure(config);
      expect(await Queue.driver.failed(), hasLength(1));
      expect(await run(<String>['prune-failed', '--hours=0']), 0);
      Queue.configure(config);
      expect(await Queue.driver.failed(), isEmpty);
    });

    test('restart stops a running worker', () async {
      // a memory cache, so the test writes no files
      Dox().cache = CacheManager(
        const CacheConfig(
          defaultStore: 'memory',
          stores: <String, Map<String, dynamic>>{
            'memory': <String, dynamic>{'driver': 'memory'},
          },
        ),
      );
      Future<int> worker = run(<String>['work']);
      await _wait(500);
      // like `restart` from another process: the shared cache changes
      await Cache().forever(QueueWorker.restartKey, 'restart 1');
      expect(
        await worker.timeout(
          QueueWorker.restartCheck + const Duration(seconds: 5),
        ),
        0,
      );
    });

    test('a worker started after restart keeps running', () async {
      Dox().cache = CacheManager(
        const CacheConfig(
          defaultStore: 'memory',
          stores: <String, Map<String, dynamic>>{
            'memory': <String, dynamic>{'driver': 'memory'},
          },
        ),
      );
      expect(await run(<String>['restart']), 0);
      bool done = false;
      Future<int> worker = run(<String>['work']).whenComplete(() {
        done = true;
      });
      // the worker reads the key at least once in this time
      await Future<void>.delayed(
        QueueWorker.restartCheck + const Duration(seconds: 1),
      );
      expect(done, isFalse);
      // a new restart still stops it
      await Cache().forever(QueueWorker.restartKey, 'restart 2');
      expect(
        await worker.timeout(
          QueueWorker.restartCheck + const Duration(seconds: 5),
        ),
        0,
      );
    });

    test('the sync driver needs no worker', () async {
      int code = await QueueWorker(app: _app(QueueConfig()))
          .run(<String>['work']);
      expect(code, 0);
    });

    test('a bad command line gives exit code 64', () async {
      expect(await run(<String>['dance']), 64);
      expect(await run(<String>['--speed=2']), 64);
      expect(await run(<String>['retry']), 64);
      expect(await run(<String>['--concurrency=0']), 64);
    });

    test('the sync driver has no failed jobs store', () async {
      Queue.configure(QueueConfig());
      int code = await QueueWorker(
        app: AppConfig(appKey: 'k' * 32, totalIsolate: 1),
      ).run(<String>['failed']);
      expect(code, 1);
    });
  });

  test('QueueCommand.parse reads the flags', () {
    QueueCommand command = QueueCommand.parse(<String>[
      'work',
      '--queue=high, default',
      '--concurrency=4',
      '--once',
      '--tries=3',
      '--timeout=30',
      '--sleep=1',
      '--stop-when-empty',
      '--timeout-grace=5',
      '--backoff=1,5',
      '--max-jobs=10',
      '--max-time=60',
      '--memory=128',
    ]);
    expect(command.name, 'work');
    WorkerOptions options = command.options;
    expect(options.queues, <String>['high', 'default']);
    expect(options.concurrency, 4);
    expect(options.once, isTrue);
    expect(options.tries, 3);
    expect(options.timeout, const Duration(seconds: 30));
    expect(options.sleep, const Duration(seconds: 1));
    expect(options.stopWhenEmpty, isTrue);
    expect(options.timeoutGrace, const Duration(seconds: 5));
    expect(options.backoff, const <Duration>[
      Duration(seconds: 1),
      Duration(seconds: 5),
    ]);
    expect(options.maxJobs, 10);
    expect(options.maxTime, const Duration(seconds: 60));
    expect(options.memory, 128);
    expect(QueueCommand.parse(<String>[]).options.timeoutGrace, isNull);
    expect(QueueCommand.parse(<String>[]).options.maxJobs, isNull);
    expect(QueueCommand.parse(<String>['prune-failed']).hours, 24);
    expect(QueueCommand.parse(<String>['prune-failed', '--hours=2']).hours, 2);
    expect(QueueCommand.parse(<String>['clear', 'emails']).argument, 'emails');
    expect(
      () => QueueCommand.parse(<String>['--backoff=1,x']),
      throwsFormatException,
    );
    expect(
      () => QueueCommand.parse(<String>['--max-jobs=0']),
      throwsFormatException,
    );
    expect(QueueCommand.parse(<String>[]).name, 'work');
    expect(QueueCommand.parse(<String>['retry', 'all']).argument, 'all');
  });

  test('backoff: the last entry is used for the rest', () {
    FlakyJob job = FlakyJob(key: 'k', failTimes: 0);
    expect(Worker.backoffFor(job, 1), const Duration(milliseconds: 150));
    expect(Worker.backoffFor(job, 5), const Duration(milliseconds: 150));
    expect(Worker.backoffFor(SendWelcomeEmail(userId: 1), 1), Duration.zero);
  });
}
