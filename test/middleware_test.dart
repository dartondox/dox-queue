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

/// a job with the middleware of [mode]: `order`, `skip` or `release`
class MiddlewareJob extends DoxJob with HasMiddleware {
  MiddlewareJob(this.mode);

  final String mode;

  @override
  int get tries => 3;

  @override
  List<JobMiddleware> get middleware => switch (mode) {
    'skip' => const <JobMiddleware>[Skip()],
    'release' => const <JobMiddleware>[Skip(releaseOnce: true)],
    _ => const <JobMiddleware>[Mark('a'), Mark('b')],
  };

  @override
  Future<void> handle() async {
    JobLog.handled.add('handle $mode');
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
  setUp(JobLog.clear);

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

    test('release() outside a worker does nothing', () async {
      await MiddlewareJob('release').dispatchSync();
      expect(JobLog.handled, isEmpty);
    });
  });
}
