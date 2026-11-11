import 'dart:convert';

import 'package:dox_queue/dox_queue.dart';
import 'package:test/test.dart';

import 'fixtures/jobs.g.dart';
import 'fixtures/test_jobs.dart';
import 'support.dart';

/// Calls `release()`, `fail()` or `delete()` in `handle()`, by [mode].
/// Logs `attempts()` of each run in `JobLog.handled`.
class ControlJob extends DoxJob {
  ControlJob(this.mode);

  /// `release-once`, `fail`, `delete`, `release-then-throw` or `plain`
  final String mode;

  @override
  int get tries => 3;

  @override
  Future<void> handle() async {
    int attempt = attempts();
    JobLog.handled.add('$mode $attempt');
    switch (mode) {
      case 'release-once':
        if (attempt == 1) release();
      case 'fail':
        // `this.`: package:test has a top-level fail()
        this.fail(StateError('stop'));
      case 'fail-default':
        this.fail();
      case 'delete':
        delete();
      case 'release-then-throw':
        if (attempt == 1) {
          release();
          throw StateError('after release');
        }
    }
  }

  @override
  void failed(Object error, StackTrace stack) {
    JobLog.failed.add('$mode: $error');
  }

  static Map<String, Object?> encode(DoxJob job) {
    return <String, Object?>{'mode': (job as ControlJob).mode};
  }

  static DoxJob decode(Map<String, Object?> data) {
    return ControlJob(data['mode'] as String);
  }
}

/// a job with a field that is not JSON
class RawJob extends DoxJob {
  RawJob(this.value);

  final Object value;

  @override
  Future<void> handle() async {
    JobLog.handled.add('raw');
  }

  static Map<String, Object?> encode(DoxJob job) {
    return <String, Object?>{'value': (job as RawJob).value};
  }

  static DoxJob decode(Map<String, Object?> data) => RawJob(data['value']!);
}

/// a job that is not in the registry
class UnknownJob extends DoxJob {
  @override
  Future<void> handle() async {
    JobLog.handled.add('unknown');
  }
}

JobRegistry registry = JobRegistry(<JobCodec>[
  ...jobRegistry.codecs,
  const JobCodec(
    'ControlJob',
    ControlJob,
    ControlJob.encode,
    ControlJob.decode,
  ),
  const JobCodec('RawJob', RawJob, RawJob.encode, RawJob.decode),
  const JobCodec(
    'DelayedRelease',
    _DelayedRelease,
    _DelayedRelease.encode,
    _DelayedRelease.decode,
  ),
  const JobCodec('SeeCopy', _SeeCopy, _SeeCopy.encode, _SeeCopy.decode),
]);

void main() {
  setUpAll(setUpTestApp);
  setUp(JobLog.clear);

  for (TestStore store in testStores) {
    group('${store.name} job control', () {
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

      test('no call: the job is deleted after handle()', () async {
        await ControlJob('plain').dispatch();
        await work();
        expect(JobLog.handled, <String>['plain 1']);
        expect(await Queue.driver.pop('default'), isNull);
        expect(await Queue.driver.failed(), isEmpty);
      });

      test('release() puts the job back, attempts() counts', () async {
        await ControlJob('release-once').dispatch();
        await work();
        expect(JobLog.handled, <String>['release-once 1', 'release-once 2']);
        expect(JobLog.failed, isEmpty);
        expect(await Queue.driver.pop('default'), isNull);
        expect(await Queue.driver.failed(), isEmpty);
      });

      test('release() with a delay waits', () async {
        await _DelayedRelease().dispatch();
        await work();
        expect(JobLog.handled, <String>['delayed 1']);
        // still waiting: not due yet
        expect(await Queue.driver.pop('default'), isNull);
        await Future<void>.delayed(const Duration(milliseconds: 400));
        ReservedJob? job = await Queue.driver.pop('default');
        expect(job?.attempts, 2);
      });

      test('fail() fails at once, with no retry', () async {
        await ControlJob('fail').dispatch();
        await work();
        expect(JobLog.handled, <String>['fail 1']);
        expect(JobLog.failed, <String>['fail: Bad state: stop']);
        List<FailedJob> failed = await Queue.driver.failed();
        expect(failed, hasLength(1));
        expect(failed.first.error, startsWith('Bad state: stop'));
      });

      test('fail() with no error says the job called it', () async {
        await ControlJob('fail-default').dispatch();
        await work();
        expect(JobLog.failed, <String>[
          'fail-default: Bad state: ControlJob called fail().',
        ]);
      });

      test('delete() deletes the job, no failed()', () async {
        await ControlJob('delete').dispatch();
        await work();
        expect(JobLog.handled, <String>['delete 1']);
        expect(JobLog.failed, isEmpty);
        expect(await Queue.driver.pop('default'), isNull);
        expect(await Queue.driver.failed(), isEmpty);
      });

      test('release() wins over an error thrown after it', () async {
        await ControlJob('release-then-throw').dispatch();
        await work();
        expect(JobLog.handled, <String>[
          'release-then-throw 1',
          'release-then-throw 2',
        ]);
        expect(JobLog.failed, isEmpty);
        expect(await Queue.driver.failed(), isEmpty);
      });
    }, skip: store.skip);
  }

  group('job control outside a worker', () {
    setUp(() => Queue.configure(QueueConfig(jobs: registry)));

    test('attempts() is 1, release() and delete() do nothing', () async {
      await ControlJob('release-once').dispatchSync();
      await ControlJob('delete').dispatch();
      expect(JobLog.handled, <String>['release-once 1', 'delete 1']);
      expect(JobLog.failed, isEmpty);
    });

    test('fail() calls failed() and throws nothing', () async {
      await ControlJob('fail').dispatchSync();
      await ControlJob('fail').dispatch();
      expect(JobLog.failed, <String>[
        'fail: Bad state: stop',
        'fail: Bad state: stop',
      ]);
    });

    test('a call on a job that does not run does nothing', () {
      ControlJob job = ControlJob('plain');
      expect(job.attempts(), 1);
      job.release();
      job.fail();
      job.delete();
      expect(JobLog.failed, isEmpty);
    });
  });

  group('sync driver', () {
    test('default: the job object runs as it is', () async {
      Queue.configure(QueueConfig(jobs: registry));
      await UnknownJob().dispatch();
      await RawJob(Object()).dispatch();
      expect(JobLog.handled, <String>['unknown', 'raw']);
    });

    test('serializeSync: a job that is not in the registry throws', () {
      Queue.configure(QueueConfig(jobs: registry, serializeSync: true));
      expect(UnknownJob().dispatch(), throwsStateError);
    });

    test('serializeSync: a field that is not JSON throws', () async {
      Queue.configure(QueueConfig(jobs: registry, serializeSync: true));
      await expectLater(
        RawJob(Object()).dispatch(),
        throwsA(isA<JsonUnsupportedObjectError>()),
      );
      expect(JobLog.handled, isEmpty);
    });

    test('serializeSync: runs a copy read back from JSON', () async {
      Queue.configure(QueueConfig(jobs: registry, serializeSync: true));
      DateTime at = DateTime.utc(2030, 1, 2, 3);
      await _SeeCopy(at).dispatch();
      expect(_SeeCopy.ran, isNot(same(_SeeCopy.dispatched)));
      expect(_SeeCopy.ran?.at, at);
    });
  });
}

/// releases with a delay of 300ms on the first run
class _DelayedRelease extends DoxJob {
  @override
  int get tries => 3;

  @override
  Future<void> handle() async {
    JobLog.handled.add('delayed ${attempts()}');
    if (attempts() == 1) release(const Duration(milliseconds: 300));
  }

  static Map<String, Object?> encode(DoxJob job) => <String, Object?>{};

  static DoxJob decode(Map<String, Object?> data) => _DelayedRelease();
}

/// keeps the job object that ran
class _SeeCopy extends DoxJob {
  _SeeCopy(this.at) {
    dispatched ??= this;
  }

  final DateTime at;

  static _SeeCopy? dispatched;
  static _SeeCopy? ran;

  @override
  Future<void> handle() async {
    ran = this;
  }

  static Map<String, Object?> encode(DoxJob job) {
    return <String, Object?>{'at': (job as _SeeCopy).at};
  }

  static DoxJob decode(Map<String, Object?> data) {
    return _SeeCopy(DateTime.parse(data['at'] as String));
  }
}
