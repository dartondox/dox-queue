import 'package:dox_queue/dox_queue.dart';
import 'package:dox_queue_database/dox_queue_database.dart';
import 'package:test/test.dart';

import 'fixtures/jobs.g.dart';
import 'fixtures/test_jobs.dart';
import 'support.dart';

/// logs [key]. [mode]: `ok`, `throw` (fails for good), `retry` (throws on
/// the first try), `release` (releases on the first try), `delete` or
/// `twice` (works, has 2 tries)
class StepJob extends DoxJob with InteractsWithQueue {
  StepJob(this.key, [this.mode = 'ok']);

  final String key;
  final String mode;

  @override
  int get tries =>
      mode == 'retry' || mode == 'release' || mode == 'twice' ? 2 : 1;

  @override
  Future<void> handle() async {
    JobLog.handled.add(key);
    if (mode == 'throw') throw StateError('$key broken');
    if (mode == 'retry' && attempts() == 1) throw StateError('$key once');
    if (mode == 'release' && attempts() == 1) release();
    if (mode == 'delete') delete();
  }

  static Map<String, Object?> encode(DoxJob job) {
    StepJob value = job as StepJob;
    return <String, Object?>{'key': value.key, 'mode': value.mode};
  }

  static DoxJob decode(Map<String, Object?> data) {
    return StepJob(data['key'] as String, data['mode'] as String);
  }
}

/// the catch job: logs the error it got
class CatchJob extends DoxJob with CatchesError {
  CatchJob(this.note);

  final String note;

  @override
  Future<void> handle() async {
    JobLog.failed.add('$note: $caughtError');
  }

  static Map<String, Object?> encode(DoxJob job) {
    return <String, Object?>{'note': (job as CatchJob).note};
  }

  static DoxJob decode(Map<String, Object?> data) {
    return CatchJob(data['note'] as String);
  }
}

class SecretStep extends DoxJob with ShouldBeEncrypted {
  SecretStep(this.secret);

  final String secret;

  @override
  Future<void> handle() async {
    JobLog.handled.add('secret $secret');
  }

  static Map<String, Object?> encode(DoxJob job) {
    return <String, Object?>{'secret': (job as SecretStep).secret};
  }

  static DoxJob decode(Map<String, Object?> data) {
    return SecretStep(data['secret'] as String);
  }
}

class UniqueStep extends DoxJob with ShouldBeUnique {
  UniqueStep(this.key);

  final String key;

  @override
  String get uniqueId => key;

  @override
  Future<void> handle() async {
    JobLog.handled.add('unique $key');
  }

  static Map<String, Object?> encode(DoxJob job) {
    return <String, Object?>{'key': (job as UniqueStep).key};
  }

  static DoxJob decode(Map<String, Object?> data) {
    return UniqueStep(data['key'] as String);
  }
}

const JobCodec stepCodec = JobCodec(
  'StepJob',
  StepJob,
  StepJob.encode,
  StepJob.decode,
);

const JobCodec catchCodec = JobCodec(
  'CatchJob',
  CatchJob,
  CatchJob.encode,
  CatchJob.decode,
);

/// a database driver whose next [fails] pushes throw
class FailingPushDriver extends DatabaseQueueDriver {
  FailingPushDriver(this.fails) : super(connection: 'sqlite');

  int fails;

  @override
  Future<void> push(JobPayload payload, {Duration delay = Duration.zero}) {
    if (fails > 0) {
      fails--;
      throw StateError('push is down');
    }
    return super.push(payload, delay: delay);
  }
}

JobRegistry registry = JobRegistry(<JobCodec>[
  ...jobRegistry.codecs,
  stepCodec,
  catchCodec,
  const JobCodec(
    'SecretStep',
    SecretStep,
    SecretStep.encode,
    SecretStep.decode,
  ),
  const JobCodec(
    'UniqueStep',
    UniqueStep,
    UniqueStep.encode,
    UniqueStep.decode,
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
    group('${store.name} chains', () {
      setUp(() async {
        await store.reset();
        Queue.configure(store.config(jobs: registry));
      });

      tearDown(() async {
        Queue.forgetListeners();
        await Queue.close();
      });

      Worker worker([JobRegistry? jobs]) {
        return Worker(
          driver: Queue.driver,
          jobs: jobs ?? registry,
          options: const WorkerOptions(stopWhenEmpty: true),
        );
      }

      Future<void> work([JobRegistry? jobs]) => worker(jobs).run();

      test('the jobs run in order, one at a time', () async {
        String id = await Queue.chain(<DoxJob>[
          StepJob('a'),
          StepJob('b'),
          StepJob('c'),
        ]).dispatch();
        expect(id, isNotEmpty);
        ReservedJob? first = await Queue.driver.pop('default');
        expect(first?.payload.id, id);
        // only the first job is on the queue, the rest is in its payload
        expect(await Queue.driver.pop('default'), isNull);
        expect(first?.payload.chain, hasLength(2));
        await worker().process(first!);
        await work();
        expect(JobLog.handled, <String>['a', 'b', 'c']);
        expect(JobLog.failed, isEmpty);
      });

      test('a failed job stops the chain and runs the catch job', () async {
        await Queue.chain(<DoxJob>[
          StepJob('a'),
          StepJob('b', 'throw'),
          StepJob('c'),
        ]).catchError(CatchJob('caught')).dispatch();
        await work();
        expect(JobLog.handled, <String>['a', 'b']);
        expect(JobLog.failed, <String>['caught: Bad state: b broken']);
        List<FailedJob> failed = await Queue.driver.failed();
        expect(failed, hasLength(1));
        expect(failed.first.job, 'StepJob');
      });

      test('the chain goes on after a retry that works', () async {
        await Queue.chain(<DoxJob>[
          StepJob('a', 'retry'),
          StepJob('b', 'release'),
          StepJob('c'),
        ]).catchError(CatchJob('caught')).dispatch();
        await work();
        expect(JobLog.handled, <String>['a', 'a', 'b', 'b', 'c']);
        expect(JobLog.failed, isEmpty);
      });

      test('delete() in a job goes on with the chain', () async {
        await Queue.chain(<DoxJob>[StepJob('a', 'delete'), StepJob('b')])
            .dispatch();
        await work();
        expect(JobLog.handled, <String>['a', 'b']);
      });

      test('two worker processes share the chain', () async {
        await Queue.chain(<DoxJob>[StepJob('a'), StepJob('b')]).dispatch();
        // each worker has its own store connection, like a process
        for (int i = 0; i < 2; i++) {
          QueueDriver driver = store.config(jobs: registry).createDriver();
          await Worker(
            driver: driver,
            jobs: registry,
            options: const WorkerOptions(once: true),
          ).run();
          await driver.close();
        }
        expect(JobLog.handled, <String>['a', 'b']);
      });

      test('an encrypted job keeps its data secret in the chain', () async {
        await Queue.chain(<DoxJob>[StepJob('a'), SecretStep('hunter2')])
            .dispatch();
        ReservedJob? first = await Queue.driver.pop('default');
        expect(first?.payload.encode(), isNot(contains('hunter2')));
        await worker().process(first!);
        await work();
        expect(JobLog.handled, <String>['a', 'secret hunter2']);
      });

      test('a skipped unique job stops the chain', () async {
        // waits on the queue, so it keeps the lock
        await UniqueStep('x').dispatch(delay: const Duration(minutes: 1));
        await Queue.chain(<DoxJob>[StepJob('a'), UniqueStep('x'), StepJob('c')])
            .catchError(CatchJob('caught'))
            .dispatch();
        await work();
        expect(JobLog.handled, <String>['a']);
        expect(
          JobLog.failed.single,
          contains('UniqueStep of a chain was skipped'),
        );
      });

      test('a next job that can not be read stops the chain', () async {
        await Queue.chain(<DoxJob>[StepJob('a'), SendWelcomeEmail(userId: 1)])
            .catchError(CatchJob('caught'))
            .dispatch();
        // this worker does not know SendWelcomeEmail
        await work(const JobRegistry(<JobCodec>[stepCodec, catchCodec]));
        expect(JobLog.handled, <String>['a']);
        expect(JobLog.failed.single, contains('SendWelcomeEmail'));
        expect(await Queue.driver.pop('default'), isNull);
      });

      test('a first job that can not be read runs the catch job', () async {
        await Queue.chain(<DoxJob>[SendWelcomeEmail(userId: 1), StepJob('b')])
            .catchError(CatchJob('caught'))
            .dispatch();
        await work(const JobRegistry(<JobCodec>[stepCodec, catchCodec]));
        expect(JobLog.handled, isEmpty);
        expect(JobLog.failed.single, contains('SendWelcomeEmail'));
      });

      test('each job of the chain fires its events', () async {
        List<String> events = <String>[];
        void log(QueueEvent event) {
          if (event.type != QueueEventType.looping) {
            events.add('${event.type.name} ${event.name ?? '-'}');
          }
        }

        for (QueueEventType type in QueueEventType.values) {
          QueueEvent.listen(type, log);
        }
        await Queue.chain(<DoxJob>[SendWelcomeEmail(userId: 1), StepJob('b')])
            .dispatch();
        await work();
        expect(events, <String>[
          'queued SendWelcomeEmail',
          'processing SendWelcomeEmail',
          'queued StepJob',
          'processed SendWelcomeEmail',
          'processing StepJob',
          'processed StepJob',
          'stopping -',
        ]);
      });
    }, skip: store.skip);
  }

  group('chains on sqlite', () {
    setUp(() async {
      await testStores.first.reset();
      Queue.configure(testStores.first.config(jobs: registry));
    });

    tearDown(Queue.close);

    Future<void> workWith(QueueDriver driver) {
      return Worker(
        driver: driver,
        jobs: registry,
        options: const WorkerOptions(stopWhenEmpty: true),
      ).run();
    }

    test('a failed push of the next job retries the job', () async {
      await Queue.chain(<DoxJob>[StepJob('a', 'twice'), StepJob('b')])
          .catchError(CatchJob('caught'))
          .dispatch();
      await workWith(FailingPushDriver(1));
      // the job runs again, then the push works
      expect(JobLog.handled, <String>['a', 'a', 'b']);
      expect(JobLog.failed, isEmpty);
    });

    test(
      'a push that keeps failing fails the job and runs the catch job',
      () async {
        await Queue.chain(<DoxJob>[StepJob('a'), StepJob('b')])
            .catchError(CatchJob('caught'))
            .dispatch();
        await workWith(FailingPushDriver(1));
        expect(JobLog.handled, <String>['a']);
        expect(JobLog.failed.single, contains('push is down'));
        expect(await Queue.driver.failed(), hasLength(1));
      },
    );

    test('a unique next job of the same id is not skipped', () async {
      await Queue.chain(<DoxJob>[UniqueStep('x'), UniqueStep('x')])
          .catchError(CatchJob('caught'))
          .dispatch();
      await workWith(Queue.driver);
      expect(JobLog.handled, <String>['unique x', 'unique x']);
      expect(JobLog.failed, isEmpty);
    });
  });

  group('chains with the sync driver', () {
    setUp(() => Queue.configure(QueueConfig(jobs: registry)));

    test('the jobs run now, in order', () async {
      String id = await Queue.chain(<DoxJob>[StepJob('a'), StepJob('b')])
          .dispatch();
      expect(id, isNotEmpty);
      expect(JobLog.handled, <String>['a', 'b']);
    });

    test('a job that throws runs the catch job and throws', () async {
      await expectLater(
        Queue.chain(<DoxJob>[StepJob('a', 'throw'), StepJob('b')])
            .catchError(CatchJob('caught'))
            .dispatch(),
        throwsStateError,
      );
      expect(JobLog.handled, <String>['a']);
      expect(JobLog.failed, <String>['caught: Bad state: a broken']);
    });
  });

  group('chains with Queue.fake()', () {
    late QueueFake fake;

    setUp(() => fake = Queue.fake());
    tearDown(Queue.restore);

    test('keeps the chain and runs nothing', () async {
      await Queue.chain(<DoxJob>[StepJob('a'), StepJob('b')]).dispatch();
      fake.assertChained(<Type>[StepJob, StepJob]);
      fake.assertNothingPushed();
      expect(JobLog.handled, isEmpty);
      expect(
        () => fake.assertChained(<Type>[StepJob]),
        throwsA(isA<AssertionError>()),
      );
      fake.clear();
      expect(fake.chains, isEmpty);
    });
  });

  test('an empty chain throws', () {
    expect(() => Queue.chain(<DoxJob>[]), throwsArgumentError);
  });

  test('a plain payload is the same as before chains', () {
    JobPayload payload = JobPayload(
      id: '1',
      job: 'StepJob',
      queue: 'default',
      data: '{}',
    );
    expect(
      payload.encode(),
      '{"id":"1","job":"StepJob","queue":"default","data":"{}",'
      '"attempts":0}',
    );
  });

  test('an old payload with no chain still reads', () {
    JobPayload payload = JobPayload.decode(
      '{"id":"1","job":"StepJob","queue":"default","data":"{}",'
      '"attempts":0}',
    );
    expect(payload.chain, isNull);
    expect(payload.chainCatch, isNull);
  });

  test('a payload keeps its chain through a retry', () {
    JobPayload payload = JobPayload(
      id: '1',
      job: 'StepJob',
      queue: 'default',
      data: '{}',
      chain: <Map<String, Object?>>[
        <String, Object?>{'job': 'StepJob', 'queue': 'default', 'data': '{}'},
      ],
      chainCatch: <String, Object?>{'job': 'CatchJob', 'data': '{}'},
    );
    JobPayload read = JobPayload.decode(
      payload.withAttempts(2).withExceptions(1).retried().encode(),
    );
    expect(read.chain, hasLength(1));
    expect(read.chainCatch?['job'], 'CatchJob');
  });
}
