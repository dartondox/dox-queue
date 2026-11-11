import 'package:dox_queue/dox_queue.dart';
import 'package:test/test.dart';

import 'fixtures/test_jobs.dart';
import 'support.dart';

/// unique by [key]. [mode]: `ok`, `throw` (fails for good) or `short`
/// (the lock ends after 300ms)
class UniqueJob extends DoxJob with ShouldBeUnique {
  UniqueJob(this.key, [this.mode = 'ok']);

  final String key;
  final String mode;

  @override
  String get uniqueId => key;

  @override
  Duration? get uniqueFor =>
      mode == 'short' ? const Duration(milliseconds: 300) : null;

  @override
  Future<void> handle() async {
    JobLog.handled.add('$key $mode');
    if (mode == 'throw') throw StateError('broken');
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

JobRegistry registry = const JobRegistry(<JobCodec>[
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
