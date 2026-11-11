import 'package:dox_orm/dox_orm.dart';
import 'package:dox_queue/dox_queue.dart';
import 'package:test/test.dart';

import 'fixtures/jobs.g.dart';
import 'fixtures/test_jobs.dart';
import 'support.dart';

/// the callback jobs that ran, as `<name>` or `<name>: <error>`
List<String> calls = <String>[];

/// A job of a batch. [mode]: `ok`, `throw` (fails for good), `retry`
/// (throws on the first try), `check` (skips its work when the batch was
/// cancelled), `add` (adds a job to its batch), `slow` (waits a bit) or
/// `flaky` (throws on its first run, works after).
class BatchStep extends DoxJob with Batchable, InteractsWithQueue {
  BatchStep(this.key, [this.mode = 'ok']);

  final String key;
  final String mode;

  @override
  int get tries => mode == 'retry' ? 2 : 1;

  @override
  Future<void> handle() async {
    if (mode == 'check' && ((await batch())?.cancelled ?? false)) {
      JobLog.handled.add('$key skipped');
      return;
    }
    JobLog.handled.add(key);
    if (mode == 'throw') throw StateError('$key broken');
    if (mode == 'flaky') {
      int run = (JobLog.runs[key] ?? 0) + 1;
      JobLog.runs[key] = run;
      if (run == 1) throw StateError('$key flaky');
    }
    if (mode == 'retry' && attempts() == 1) throw StateError('$key once');
    if (mode == 'add') await (await batch())!.add(<DoxJob>[BatchStep('extra')]);
    if (mode == 'slow') {
      await Future<void>.delayed(const Duration(milliseconds: 20));
    }
  }

  static Map<String, Object?> encode(DoxJob job) {
    BatchStep value = job as BatchStep;
    return <String, Object?>{'key': value.key, 'mode': value.mode};
  }

  static DoxJob decode(Map<String, Object?> data) {
    return BatchStep(data['key'] as String, data['mode'] as String);
  }
}

/// a callback job of a batch: logs its name, the batch id and the error.
/// The name `add` tries to add a job to the batch and logs the error.
class Callback extends DoxJob with Batchable, CatchesError {
  Callback(this.name);

  final String name;

  @override
  Future<void> handle() async {
    Object? error = caughtError;
    calls.add(error == null ? name : '$name: $error');
    if (batchId == null) calls.add('$name has no batch');
    if (name == 'add') {
      try {
        await (await batch())!.add(<DoxJob>[BatchStep('late')]);
      } on StateError {
        calls.add('add refused');
      }
    }
  }

  static Map<String, Object?> encode(DoxJob job) {
    return <String, Object?>{'name': (job as Callback).name};
  }

  static DoxJob decode(Map<String, Object?> data) {
    return Callback(data['name'] as String);
  }
}

class UniqueBatchStep extends DoxJob with ShouldBeUnique {
  UniqueBatchStep();

  @override
  Future<void> handle() async {
    JobLog.handled.add('unique');
  }

  static Map<String, Object?> encode(DoxJob job) => <String, Object?>{};

  static DoxJob decode(Map<String, Object?> data) => UniqueBatchStep();
}

JobRegistry registry = JobRegistry(<JobCodec>[
  ...jobRegistry.codecs,
  const JobCodec('BatchStep', BatchStep, BatchStep.encode, BatchStep.decode),
  const JobCodec('Callback', Callback, Callback.encode, Callback.decode),
  const JobCodec(
    'UniqueBatchStep',
    UniqueBatchStep,
    UniqueBatchStep.encode,
    UniqueBatchStep.decode,
  ),
]);

/// a batch with every callback
PendingBatch batchOf(List<DoxJob> jobs) {
  return Queue.batch(jobs)
      .name('test')
      .before(Callback('before'))
      .progress(Callback('progress'))
      .then(Callback('then'))
      .catchError(Callback('catch'))
      .whenComplete(Callback('finally'));
}

final Set<String> _made = <String>{};

/// make and empty the job_batches table of the dox_orm connection [name]
/// (null: the default connection)
Future<void> resetBatches(String? name) async {
  DBDriver db = SqlQueryBuilder().getDBDriver(name);
  if (_made.add(name ?? '')) {
    Schema schema = Schema(db);
    await QueueTables.downBatches(schema);
    await QueueTables.upBatches(schema);
  }
  await db.execute('DELETE FROM job_batches');
}

void main() {
  setUpAll(setUpTestApp);
  setUp(() async {
    JobLog.clear();
    calls.clear();
    await useTestCache();
  });
  tearDownAll(closeTestCache);

  for (TestStore store in testStores) {
    group('${store.name} batches', () {
      setUp(() async {
        await store.reset();
        // the Redis driver keeps batches in the default connection
        await resetBatches(store.isRedis ? null : store.name);
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

      test('counts the jobs and runs then and finally once', () async {
        Batch batch = await batchOf(<DoxJob>[
          BatchStep('a'),
          BatchStep('b'),
          BatchStep('c', 'retry'),
        ]).dispatch();
        expect(batch.name, 'test');
        expect(batch.totalJobs, 3);
        expect(batch.pendingJobs, 3);
        expect(batch.progress, 0);
        expect(batch.finished, isFalse);
        expect(calls, <String>['before']);
        await work();
        Batch? done = await Queue.findBatch(batch.id);
        expect(done?.pendingJobs, 0);
        expect(done?.processedJobs, 3);
        expect(done?.failedJobs, 0);
        expect(done?.progress, 100);
        expect(done?.finished, isTrue);
        expect(done?.cancelled, isFalse);
        expect(JobLog.handled, <String>['a', 'b', 'c', 'c']);
        expect(calls, <String>[
          'before',
          'progress',
          'progress',
          'progress',
          'then',
          'finally',
        ]);
      });

      test('two workers at once count each job once', () async {
        Batch batch = await batchOf(<DoxJob>[
          for (int i = 0; i < 20; i++) BatchStep('$i', 'slow'),
        ]).dispatch();
        List<QueueDriver> drivers = <QueueDriver>[
          for (int i = 0; i < 2; i++)
            store.config(jobs: registry).createDriver(),
        ];
        await Future.wait(<Future<void>>[
          for (QueueDriver driver in drivers)
            Worker(
              driver: driver,
              jobs: registry,
              options: const WorkerOptions(stopWhenEmpty: true, concurrency: 3),
            ).run(),
        ]);
        for (QueueDriver driver in drivers) {
          await driver.close();
        }
        Batch? done = await Queue.findBatch(batch.id);
        expect(JobLog.handled, hasLength(20));
        expect(done?.pendingJobs, 0);
        expect(done?.finished, isTrue);
        expect(calls.where((String call) => call == 'progress'), hasLength(20));
        expect(calls.where((String call) => call == 'then'), hasLength(1));
        expect(calls.where((String call) => call == 'finally'), hasLength(1));
      });

      test('the first failure cancels the batch', () async {
        Batch batch = await batchOf(<DoxJob>[
          BatchStep('a'),
          BatchStep('b', 'throw'),
          BatchStep('c', 'check'),
        ]).dispatch();
        await work();
        Batch? done = await Queue.findBatch(batch.id);
        expect(JobLog.handled, <String>['a', 'b', 'c skipped']);
        expect(done?.cancelled, isTrue);
        expect(done?.finished, isTrue);
        expect(done?.failedJobs, 1);
        // a failed job still counts as pending: it can be retried
        expect(done?.pendingJobs, 1);
        expect(calls, <String>[
          'before',
          'progress',
          'catch: Bad state: b broken',
          'progress',
          'finally',
        ]);
      });

      test('allowFailures keeps the batch going', () async {
        Batch batch = await batchOf(<DoxJob>[
          BatchStep('a', 'throw'),
          BatchStep('b', 'throw'),
          BatchStep('c', 'check'),
        ]).allowFailures().dispatch();
        await work();
        Batch? done = await Queue.findBatch(batch.id);
        expect(JobLog.handled, <String>['a', 'b', 'c']);
        expect(done?.cancelled, isFalse);
        expect(done?.failedJobs, 2);
        expect(done?.finished, isTrue);
        expect(calls, <String>[
          'before',
          'progress',
          'catch: Bad state: a broken',
          'progress',
          'progress',
          'finally',
        ]);
      });

      test('cancel() marks the batch, its jobs can skip', () async {
        Batch batch = await Queue.batch(<DoxJob>[BatchStep('a', 'check')])
            .dispatch();
        await batch.cancel();
        await work();
        Batch? done = await Queue.findBatch(batch.id);
        expect(done?.cancelled, isTrue);
        expect(done?.finished, isTrue);
        expect(JobLog.handled, <String>['a skipped']);
      });

      test('add() from a job of the batch', () async {
        Batch batch = await batchOf(<DoxJob>[BatchStep('a', 'add')]).dispatch();
        await work();
        Batch? done = await Queue.findBatch(batch.id);
        expect(JobLog.handled, <String>['a', 'extra']);
        expect(done?.totalJobs, 2);
        expect(done?.pendingJobs, 0);
        expect(calls, <String>[
          'before',
          'progress',
          'progress',
          'then',
          'finally',
        ]);
      });

      test('a skipped unique job is not counted', () async {
        await UniqueBatchStep().dispatch(delay: const Duration(minutes: 1));
        Batch batch = await batchOf(<DoxJob>[BatchStep('a'), UniqueBatchStep()])
            .dispatch();
        expect(batch.totalJobs, 1);
        expect(batch.pendingJobs, 1);
        // no job ran: no progress
        expect(calls, <String>['before']);
      });

      test('a batch of only a skipped unique job ends at once', () async {
        await UniqueBatchStep().dispatch(delay: const Duration(minutes: 1));
        Batch batch = await batchOf(<DoxJob>[UniqueBatchStep()]).dispatch();
        expect(batch.totalJobs, 0);
        expect(batch.finished, isTrue);
        expect(calls, <String>['before', 'then', 'finally']);
      });

      Worker once() {
        return Worker(
          driver: Queue.driver,
          jobs: registry,
          options: const WorkerOptions(once: true),
        );
      }

      Future<String> failedId() async {
        return (await Queue.driver.failed()).single.id;
      }

      test('a retried job that fails again is counted once', () async {
        Batch batch = await batchOf(<DoxJob>[
          BatchStep('a', 'throw'),
          BatchStep('b'),
        ]).allowFailures().dispatch();
        await once().run(); // a fails
        ReservedJob? b = await Queue.driver.pop('default');
        expect(await Queue.driver.retry(await failedId()), isTrue);
        await once().run(); // a fails again
        Batch? now = await Queue.findBatch(batch.id);
        expect(now?.failedJobs, 1);
        expect(now?.pendingJobs, 2);
        expect(now?.finished, isFalse);
        await once().process(b!);
        now = await Queue.findBatch(batch.id);
        expect(now?.finished, isTrue);
        expect(calls, <String>[
          'before',
          'progress',
          'catch: Bad state: a broken',
          'progress',
          'finally',
        ]);
      });

      test('a retried job that works is no longer failed', () async {
        Batch batch = await batchOf(<DoxJob>[
          BatchStep('a', 'flaky'),
          BatchStep('b'),
        ]).allowFailures().dispatch();
        await once().run(); // a fails
        ReservedJob? b = await Queue.driver.pop('default');
        expect(await Queue.driver.retry(await failedId()), isTrue);
        await once().run(); // a works now
        Batch? now = await Queue.findBatch(batch.id);
        expect(now?.failedJobs, 0);
        expect(now?.pendingJobs, 1);
        expect(now?.finished, isFalse);
        await once().process(b!);
        now = await Queue.findBatch(batch.id);
        expect(now?.pendingJobs, 0);
        expect(now?.finished, isTrue);
        expect(calls, <String>[
          'before',
          'progress',
          'catch: Bad state: a flaky',
          'progress',
          'progress',
          'then',
          'finally',
        ]);
      });

      test('add() from a then job throws', () async {
        await Queue.batch(<DoxJob>[BatchStep('a')])
            .then(Callback('add'))
            .dispatch();
        await work();
        expect(calls, <String>['add', 'add refused']);
        expect(JobLog.handled, <String>['a']);
      });

      test('add() to a finished batch throws and pushes nothing', () async {
        Batch batch = await Queue.batch(<DoxJob>[BatchStep('a')]).dispatch();
        await work();
        await expectLater(
          batch.add(<DoxJob>[BatchStep('b')]),
          throwsA(isA<StateError>()),
        );
        expect(await Queue.driver.pop('default'), isNull);
        Batch? now = await Queue.findBatch(batch.id);
        expect(now?.totalJobs, 1);
      });

      test('the jobs carry the batch id, a plain job does not', () async {
        Batch batch = await Queue.batch(<DoxJob>[BatchStep('a')]).dispatch();
        await SendWelcomeEmail(userId: 1).dispatch();
        ReservedJob? inBatch = await Queue.driver.pop('default');
        ReservedJob? plain = await Queue.driver.pop('default');
        expect(inBatch?.payload.batch, batch.id);
        expect(plain?.payload.batch, isNull);
        expect(plain?.payload.encode(), isNot(contains('batch')));
      });
    }, skip: store.skip);
  }

  group('batches on sqlite', () {
    setUp(() async {
      await testStores.first.reset();
      await resetBatches('sqlite');
      Queue.configure(testStores.first.config(jobs: registry));
    });

    tearDown(Queue.close);

    test('pruneBatches deletes old finished batches', () async {
      Batch done = await Queue.batch(<DoxJob>[BatchStep('a')]).dispatch();
      Batch open = await Queue.batch(<DoxJob>[BatchStep('b')]).dispatch();
      await done.cancel();
      DateTime later = DateTime.now().add(const Duration(hours: 1));
      expect(await Queue.pruneBatches(later), 1);
      expect(await Queue.findBatch(done.id), isNull);
      expect(await Queue.findBatch(open.id), isNotNull);
    });
  });

  group('batches on sqlite, errors', () {
    setUp(() async {
      await testStores.first.reset();
      await resetBatches('sqlite');
      Queue.configure(
        QueueConfig(
          driver: 'database',
          jobs: registry,
          connection: 'sqlite',
          connections: <String, QueueConfig>{
            // no jobs table there: every push throws
            'broken': QueueConfig(
              driver: 'database',
              connection: 'sqlite_other',
            ),
          },
        ),
      );
    });

    tearDown(Queue.close);

    test('an empty batch throws', () {
      expect(() => Queue.batch(<DoxJob>[]), throwsArgumentError);
    });

    test('a push that throws cancels the batch', () async {
      await expectLater(
        Queue.batch(<DoxJob>[BatchStep('a'), BatchStep('b')])
            .then(Callback('then'))
            .dispatch(connection: 'broken'),
        throwsA(anything),
      );
      List<Map<String, dynamic>> rows = await SqlQueryBuilder()
          .getDBDriver('sqlite')
          .query('SELECT id FROM job_batches');
      Batch? batch = await Queue.findBatch('${rows.single['id']}');
      expect(batch?.totalJobs, 0);
      expect(batch?.pendingJobs, 0);
      expect(batch?.cancelled, isTrue);
      expect(batch?.finished, isTrue);
      expect(calls, isEmpty);
    });

    test('add() to a deleted batch throws', () async {
      Batch batch = await Queue.batch(<DoxJob>[BatchStep('a')]).dispatch();
      await SqlQueryBuilder()
          .getDBDriver('sqlite')
          .execute('DELETE FROM job_batches');
      await expectLater(
        batch.add(<DoxJob>[BatchStep('b')]),
        throwsA(isA<StateError>()),
      );
    });

    test('pruneBatches can delete old unfinished batches', () async {
      Batch open = await Queue.batch(<DoxJob>[BatchStep('a')]).dispatch();
      DateTime later = DateTime.now().add(const Duration(hours: 1));
      expect(await Queue.pruneBatches(later), 0);
      expect(await Queue.pruneBatches(later, unfinishedBefore: later), 1);
      expect(await Queue.findBatch(open.id), isNull);
    });
  });

  test('a missing job_batches table gives a clear error', () async {
    Queue.configure(QueueConfig(jobs: registry, connection: 'sqlite_other'));
    await expectLater(
      Queue.batch(<DoxJob>[BatchStep('a')]).dispatch(),
      throwsA(
        isA<StateError>().having(
          (StateError error) => error.message,
          'message',
          contains('QueueTables.upBatches()'),
        ),
      ),
    );
  });

  group('batches with the sync driver', () {
    setUp(() async {
      await resetBatches(null);
      Queue.configure(QueueConfig(jobs: registry));
    });

    test('the jobs run now and are counted', () async {
      Batch batch = await batchOf(<DoxJob>[
        BatchStep('a'),
        BatchStep('b', 'throw'),
      ]).allowFailures().dispatch();
      expect(JobLog.handled, <String>['a', 'b']);
      expect(batch.pendingJobs, 1);
      expect(batch.failedJobs, 1);
      expect(batch.finished, isTrue);
      expect(calls, <String>[
        'before',
        'progress',
        'progress',
        'catch: Bad state: b broken',
        'finally',
      ]);
    });
  });

  group('batches with Queue.fake()', () {
    late QueueFake fake;

    setUp(() => fake = Queue.fake());
    tearDown(Queue.restore);

    test('keeps the batch and runs nothing', () async {
      fake.assertBatchCount(0);
      Batch batch = await Queue.batch(<DoxJob>[BatchStep('a')])
          .name('import')
          .dispatch();
      await batch.add(<DoxJob>[BatchStep('b')]);
      await batch.cancel();
      fake.assertBatched();
      fake.assertBatched(
        (PendingBatch batch) =>
            batch.batchName == 'import' && batch.jobs.length == 2,
      );
      fake.assertBatchCount(1);
      fake.assertNothingPushed();
      expect(JobLog.handled, isEmpty);
      expect(await Queue.findBatch(batch.id), isNull);
      expect(
        () => fake.assertBatched((PendingBatch batch) => false),
        throwsA(isA<AssertionError>()),
      );
      expect(
        () => fake.assertBatchCount(2),
        throwsA(
          isA<AssertionError>().having(
            (AssertionError error) => error.message,
            'message',
            'Expected 2 batches to be dispatched, but 1 was.',
          ),
        ),
      );
    });
  });

  test('an old payload with no batch still reads', () {
    JobPayload payload = JobPayload.decode(
      '{"id":"1","job":"BatchStep","queue":"default","data":"{}",'
      '"attempts":0}',
    );
    expect(payload.batch, isNull);
    expect(payload.batchFailed, isFalse);
  });

  test('a retried batch job is marked, a plain one is not', () {
    JobPayload plain = JobPayload(
      id: '1',
      job: 'BatchStep',
      queue: 'default',
      data: '{}',
    ).retried();
    expect(plain.encode(), isNot(contains('batchFailed')));
    JobPayload inBatch = JobPayload(
      id: '1',
      job: 'BatchStep',
      queue: 'default',
      data: '{}',
      batch: 'b1',
    ).retried();
    expect(JobPayload.decode(inBatch.encode()).batchFailed, isTrue);
  });
}
