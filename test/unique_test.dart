import 'package:dox_core/dox_core.dart' show CacheConfig, CacheManager, Dox;
import 'package:dox_orm/dox_orm.dart' show QueryBuilder;
import 'package:dox_queue/dox_queue.dart';
import 'package:test/test.dart';

import 'fixtures/jobs.g.dart';
import 'fixtures/test_jobs.dart';
import 'support.dart';

/// unique by [key]. [mode]: `ok`, `throw` (fails for good), `short`
/// (the lock ends after 300ms), `release` and `retry` (release or throw
/// on the first try) or `delete`
class UniqueJob extends DoxJob with ShouldBeUnique, InteractsWithQueue {
  UniqueJob(this.key, [this.mode = 'ok']);

  final String key;
  final String mode;

  @override
  String get uniqueId => key;

  @override
  Duration? get uniqueFor =>
      mode == 'short' ? const Duration(milliseconds: 300) : null;

  @override
  int get tries => mode == 'release' || mode == 'retry' ? 2 : 1;

  @override
  Future<void> handle() async {
    JobLog.handled.add('$key $mode');
    if (mode == 'throw') throw StateError('broken');
    if (mode == 'release' && attempts() == 1) release();
    if (mode == 'retry' && attempts() == 1) throw StateError('once');
    if (mode == 'delete') delete();
  }

  static Map<String, Object?> encode(DoxJob job) {
    UniqueJob value = job as UniqueJob;
    return <String, Object?>{'key': value.key, 'mode': value.mode};
  }

  static DoxJob decode(Map<String, Object?> data) {
    return UniqueJob(data['key'] as String, data['mode'] as String);
  }
}

/// While it runs, it dispatches a copy of itself and logs the id.
class UntilProcessingJob extends DoxJob with ShouldBeUniqueUntilProcessing {
  UntilProcessingJob();

  @override
  Future<void> handle() async {
    String id = await UntilProcessingJob().dispatch();
    JobLog.handled.add(id.isEmpty ? 'skipped' : 'pushed');
  }

  static Map<String, Object?> encode(DoxJob job) => <String, Object?>{};

  static DoxJob decode(Map<String, Object?> data) => UntilProcessingJob();
}

JobRegistry registry = JobRegistry(<JobCodec>[
  ...jobRegistry.codecs,
  JobCodec('UniqueJob', UniqueJob, UniqueJob.encode, UniqueJob.decode),
  JobCodec(
    'UntilProcessingJob',
    UntilProcessingJob,
    UntilProcessingJob.encode,
    UntilProcessingJob.decode,
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
    group('${store.name} unique jobs', () {
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

      test('a second job with the same id is skipped', () async {
        String first = await UniqueJob('a').dispatch();
        String second = await UniqueJob('a').dispatch();
        String other = await UniqueJob('b').dispatch();
        expect(first, isNotEmpty);
        expect(second, isEmpty);
        expect(other, isNotEmpty);
        await work();
        expect(JobLog.handled, <String>['a ok', 'b ok']);
      });

      test('the lock is freed after the job is done', () async {
        await UniqueJob('a').dispatch();
        await work();
        expect(await UniqueJob('a').dispatch(), isNotEmpty);
      });

      test('the lock is freed after the job failed for good', () async {
        await UniqueJob('a', 'throw').dispatch();
        await work();
        expect(await Queue.driver.failed(), hasLength(1));
        expect(await UniqueJob('a').dispatch(), isNotEmpty);
      });

      test('the lock ends after uniqueFor', () async {
        await UniqueJob('a', 'short').dispatch();
        expect(await UniqueJob('a', 'short').dispatch(), isEmpty);
        await Future<void>.delayed(const Duration(milliseconds: 400));
        expect(await UniqueJob('a', 'short').dispatch(), isNotEmpty);
        await work();
        expect(JobLog.handled, <String>['a short', 'a short']);
      });

      Future<void> workOnce([JobRegistry? jobs]) {
        return Worker(
          driver: Queue.driver,
          jobs: jobs ?? registry,
          options: const WorkerOptions(once: true),
        ).run();
      }

      test('release() keeps the lock', () async {
        await UniqueJob('a', 'release').dispatch();
        await workOnce();
        expect(await UniqueJob('a').dispatch(), isEmpty);
      });

      test('a retry keeps the lock', () async {
        await UniqueJob('a', 'retry').dispatch();
        await workOnce();
        expect(await UniqueJob('a').dispatch(), isEmpty);
      });

      test('delete() in the job frees the lock', () async {
        await UniqueJob('a', 'delete').dispatch();
        await work();
        expect(await UniqueJob('a').dispatch(), isNotEmpty);
      });

      test('an old job does not free the lock of a newer job', () async {
        await UniqueJob('a', 'short').dispatch();
        await Future<void>.delayed(const Duration(milliseconds: 400));
        // the first lock ended: a new job takes a new lock
        expect(await UniqueJob('a').dispatch(), isNotEmpty);
        await workOnce(); // runs the old job
        expect(JobLog.handled, <String>['a short']);
        expect(await UniqueJob('a').dispatch(), isEmpty);
      });

      test('clear() frees the locks of the jobs it deletes', () async {
        await UniqueJob('a').dispatch();
        await UniqueJob('b').dispatch(delay: const Duration(minutes: 1));
        await SendWelcomeEmail(userId: 1).dispatch();
        expect(await Queue.driver.clear('default'), 3);
        expect(await UniqueJob('a').dispatch(), isNotEmpty);
        expect(await UniqueJob('b').dispatch(), isNotEmpty);
      });

      test('a job that can not be read frees its lock', () async {
        await UniqueJob('a').dispatch();
        await workOnce(const JobRegistry(<JobCodec>[]));
        expect(await Queue.driver.failed(), hasLength(1));
        expect(await UniqueJob('a').dispatch(), isNotEmpty);
      });

      test('the payload keeps the lock name, a plain job has none', () async {
        await UniqueJob('a').dispatch();
        await SendWelcomeEmail(userId: 1).dispatch();
        ReservedJob? unique = await Queue.driver.pop('default');
        ReservedJob? plain = await Queue.driver.pop('default');
        expect(unique?.payload.unique, 'dox_queue:unique:UniqueJob:a');
        expect(plain?.payload.unique, isNull);
        expect(plain?.payload.encode(), isNot(contains('unique')));
      });

      test('until processing: the lock is freed when the job starts', () async {
        await UntilProcessingJob().dispatch();
        expect(await UntilProcessingJob().dispatch(), isEmpty);
        await Worker(
          driver: Queue.driver,
          jobs: registry,
          options: const WorkerOptions(once: true),
        ).run();
        expect(JobLog.handled, <String>['pushed']);
      });
    }, skip: store.skip);
  }

  group('unique jobs on sqlite', () {
    TestStore store = testStores.first;

    setUp(() async {
      await store.reset();
      Queue.configure(store.config(jobs: registry));
    });

    tearDown(Queue.close);

    test('afterCommit takes the lock at the commit', () async {
      String inside = '';
      await QueryBuilder.transaction(() async {
        await UniqueJob('a').dispatch(afterCommit: true);
        // no lock yet: this one is pushed
        inside = await UniqueJob('a').dispatch();
      });
      expect(inside, isNotEmpty);
      // the lock was taken: the job of afterCommit was skipped
      expect(await Queue.driver.pop('default'), isNotNull);
      expect(await Queue.driver.pop('default'), isNull);
    });

    test('a push that fails frees the lock', () async {
      Queue.configure(
        QueueConfig(
          driver: 'database',
          jobs: registry,
          connection: 'sqlite',
          connections: <String, QueueConfig>{
            // no jobs table there
            'broken': QueueConfig(
              driver: 'database',
              connection: 'sqlite_other',
            ),
          },
        ),
      );
      await expectLater(
        UniqueJob('a').dispatch(connection: 'broken'),
        throwsA(anything),
      );
      expect(await UniqueJob('a').dispatch(), isNotEmpty);
    });

    test('a plain job never uses the cache', () async {
      // a cache that throws on use
      Dox().cache = CacheManager(
        const CacheConfig(
          defaultStore: 'none',
          stores: <String, Map<String, dynamic>>{},
        ),
      );
      await SendWelcomeEmail(userId: 1).dispatch();
      await Worker(
        driver: Queue.driver,
        jobs: registry,
        options: const WorkerOptions(stopWhenEmpty: true),
      ).run();
      Queue.configure(QueueConfig(jobs: registry));
      await SendWelcomeEmail(userId: 2).dispatch();
      expect(JobLog.handled, <String>['welcome 1', 'welcome 2']);
    });
  });

  test('an old payload with no lock name still reads', () {
    JobPayload payload = JobPayload.decode(
      '{"id":"1","job":"UniqueJob","queue":"default","data":"{}",'
      '"attempts":0}',
    );
    expect(payload.unique, isNull);
  });

  group('unique jobs with the sync driver', () {
    setUp(() => Queue.configure(QueueConfig(jobs: registry)));

    test('the job runs and frees its lock', () async {
      expect(await UniqueJob('a').dispatch(), isNotEmpty);
      expect(await UniqueJob('a').dispatch(), isNotEmpty);
      expect(JobLog.handled, <String>['a ok', 'a ok']);
    });

    test('the lock is freed when the job throws', () async {
      await expectLater(UniqueJob('a', 'throw').dispatch(), throwsStateError);
      expect(await UniqueJob('a').dispatch(), isNotEmpty);
    });
  });

  group('unique jobs with Queue.fake()', () {
    late QueueFake fake;

    setUp(() => fake = Queue.fake());
    tearDown(Queue.restore);

    test('a second job with the same id is not pushed', () async {
      expect(await UniqueJob('a').dispatch(), isNotEmpty);
      expect(await UniqueJob('a').dispatch(), isEmpty);
      expect(await UniqueJob('b').dispatch(), isNotEmpty);
      fake.assertPushedTimes<UniqueJob>(2);
    });

    test('clear() frees the locks, uniqueFor ends them', () async {
      await UniqueJob('a').dispatch();
      fake.clear();
      expect(await UniqueJob('a').dispatch(), isNotEmpty);
      await UniqueJob('b', 'short').dispatch();
      await Future<void>.delayed(const Duration(milliseconds: 400));
      expect(await UniqueJob('b', 'short').dispatch(), isNotEmpty);
    });
  });
}
