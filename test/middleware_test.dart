import 'package:dox_core/dox_core.dart'
    show AppConfig, Cache, Dox, LoggerConfig;
import 'package:dox_queue/dox_queue.dart';
import 'package:test/test.dart';

import 'fixtures/test_jobs.dart';
import 'support.dart';

/// logs `<name> before` and `<name> after` around the rest
class Mark extends JobMiddleware {
  const Mark(this.name);

  final String name;

  @override
  Future<void> handle(DoxJob job, JobNext next) async {
    JobLog.handled.add('$name before');
    await next();
    JobLog.handled.add('$name after');
  }
}

/// skips the job; releases it first on the first try with [release]
class Skip extends JobMiddleware {
  const Skip({this.releaseOnce = false});

  final bool releaseOnce;

  @override
  Future<void> handle(DoxJob job, JobNext next) async {
    if (releaseOnce && JobLog.runs['released'] == null) {
      JobLog.runs['released'] = 1;
      release(job);
      return;
    }
    if (!releaseOnce) return;
    await next();
  }
}

/// a job with the middleware of [mode]: `order`, `skip`, `release`,
/// `limit`, `overlap-wait`, `overlap-drop` or `throttle`
class MiddlewareJob extends DoxJob with HasMiddleware {
  MiddlewareJob(this.mode);

  final String mode;

  @override
  int get tries => 3;

  @override
  List<JobMiddleware> get middleware => switch (mode) {
    'skip' => const <JobMiddleware>[Skip()],
    'release' => const <JobMiddleware>[Skip(releaseOnce: true)],
    'limit' => const <JobMiddleware>[
      RateLimited('mail', 2, Duration(seconds: 1)),
    ],
    'overlap-wait' => const <JobMiddleware>[
      WithoutOverlapping('report', releaseAfter: Duration(seconds: 1)),
    ],
    'overlap-drop' => const <JobMiddleware>[
      WithoutOverlapping('report', releaseAfter: null),
    ],
    'throttle' => const <JobMiddleware>[
      ThrottlesExceptions('api', maxAttempts: 1),
    ],
    'throttle-once' => const <JobMiddleware>[
      ThrottlesExceptions('api-once', maxAttempts: 2),
    ],
    _ => const <JobMiddleware>[Mark('a'), Mark('b')],
  };

  @override
  Future<void> handle() async {
    JobLog.handled.add('handle $mode');
    if (mode.startsWith('overlap')) {
      await Future<void>.delayed(const Duration(milliseconds: 100));
    }
    if (mode == 'throttle') throw StateError('the api is down');
    if (mode == 'throttle-once' && JobLog.handled.length == 1) {
      throw StateError('the api is down once');
    }
  }

  static Map<String, Object?> encode(DoxJob job) {
    return <String, Object?>{'mode': (job as MiddlewareJob).mode};
  }

  static DoxJob decode(Map<String, Object?> data) {
    return MiddlewareJob(data['mode'] as String);
  }
}

JobRegistry registry = const JobRegistry(<JobCodec>[
  JobCodec(
    'MiddlewareJob',
    MiddlewareJob,
    MiddlewareJob.encode,
    MiddlewareJob.decode,
  ),
]);

void main() {
  setUpAll(setUpTestApp);
  setUp(() async {
    JobLog.clear();
    await useTestCache();
  });
  tearDownAll(closeTestCache);

  for (TestStore store in testStores) {
    group('${store.name} middleware', () {
      setUp(() async {
        await store.reset();
        Queue.configure(store.config(jobs: registry));
      });

      tearDown(Queue.close);

      Future<void> work() {
        return Worker(
          driver: Queue.driver,
          jobs: registry,
          options: const WorkerOptions(stopWhenEmpty: true),
        ).run();
      }

      /// two workers at once, like two worker processes
      Future<void> workTwice() {
        return Future.wait(<Future<void>>[work(), work()]);
      }

      test('runs in order, the first one outside', () async {
        await MiddlewareJob('order').dispatch();
        await work();
        expect(JobLog.handled, <String>[
          'a before',
          'b before',
          'handle order',
          'b after',
          'a after',
        ]);
        expect(await Queue.driver.pop('default'), isNull);
      });

      test('a middleware that does not call next skips the job', () async {
        await MiddlewareJob('skip').dispatch();
        await work();
        expect(JobLog.handled, isEmpty);
        expect(await Queue.driver.pop('default'), isNull);
        expect(await Queue.driver.failed(), isEmpty);
      });

      test('a middleware can release the job', () async {
        await MiddlewareJob('release').dispatch();
        await work();
        // the first try was released, the second ran
        expect(JobLog.handled, <String>['handle release']);
        expect(await Queue.driver.pop('default'), isNull);
        expect(await Queue.driver.failed(), isEmpty);
      });

      test('RateLimited: the limit counts for all workers', () async {
        for (int i = 0; i < 5; i++) {
          await MiddlewareJob('limit').dispatch();
        }
        await workTwice();
        // 2 ran, 3 wait for the next window
        expect(JobLog.handled, hasLength(2));
        expect(await Queue.driver.failed(), isEmpty);
        expect(await Queue.driver.pop('default'), isNull);
        await Future<void>.delayed(const Duration(milliseconds: 1100));
        await workTwice();
        expect(JobLog.handled, hasLength(4));
      });

      test('WithoutOverlapping: the other job waits', () async {
        await MiddlewareJob('overlap-wait').dispatch();
        await MiddlewareJob('overlap-wait').dispatch();
        await workTwice();
        expect(JobLog.handled, <String>['handle overlap-wait']);
        expect(await Queue.driver.pop('default'), isNull);
        await Future<void>.delayed(const Duration(milliseconds: 1100));
        await work();
        expect(JobLog.handled, hasLength(2));
        expect(await Queue.driver.failed(), isEmpty);
      });

      test('WithoutOverlapping: releaseAfter null drops the job', () async {
        await MiddlewareJob('overlap-drop').dispatch();
        await MiddlewareJob('overlap-drop').dispatch();
        await workTwice();
        expect(JobLog.handled, <String>['handle overlap-drop']);
        await Future<void>.delayed(const Duration(milliseconds: 1100));
        expect(await Queue.driver.pop('default'), isNull);
        expect(await Queue.driver.failed(), isEmpty);
      });

      test('ThrottlesExceptions: after the limit the job waits', () async {
        await MiddlewareJob('throttle').dispatch();
        await work();
        // try 1 threw and was released, try 2 found the limit hit and
        // was released until the window ends: no failed job
        expect(JobLog.handled, <String>['handle throttle']);
        expect(await Queue.driver.failed(), isEmpty);
        expect(await Queue.driver.pop('default'), isNull);
      });
    }, skip: store.skip);
  }

  group('middleware outside a worker', () {
    setUp(() => Queue.configure(QueueConfig(jobs: registry)));

    test('dispatchSync() and the sync driver use it', () async {
      await MiddlewareJob('order').dispatchSync();
      await MiddlewareJob('order').dispatch();
      expect(JobLog.handled, hasLength(10));
      expect(JobLog.handled[2], 'handle order');
    });

    test('ThrottlesExceptions: the error goes on, then it skips', () async {
      await expectLater(
        MiddlewareJob('throttle').dispatchSync(),
        throwsStateError,
      );
      await MiddlewareJob('throttle').dispatchSync();
      expect(JobLog.handled, <String>['handle throttle']);
    });

    test('ThrottlesExceptions: a run with no error resets it', () async {
      await expectLater(
        MiddlewareJob('throttle-once').dispatchSync(),
        throwsStateError,
      );
      expect(await Cache().has('dox_queue:throttle:api-once:timer'), isTrue);
      await MiddlewareJob('throttle-once').dispatchSync();
      expect(JobLog.handled, hasLength(2));
      expect(await Cache().has('dox_queue:throttle:api-once'), isFalse);
      expect(await Cache().has('dox_queue:throttle:api-once:timer'), isFalse);
    });

    test('a release outside a worker logs a warning', () async {
      Dox().initialize(
        AppConfig(
          appKey: 'k' * 32,
          totalIsolate: 1,
          logger: LoggerConfig(enabled: true),
        ),
      );
      await useTestCache();
      String output;
      try {
        output = await logOf(MiddlewareJob('release').dispatchSync);
      } finally {
        Dox().initialize(AppConfig(appKey: 'k' * 32, totalIsolate: 1));
      }
      expect(output, contains('Skip released MiddlewareJob outside a worker'));
    });

    test('release() outside a worker does nothing', () async {
      await MiddlewareJob('release').dispatchSync();
      expect(JobLog.handled, isEmpty);
    });
  });
}
