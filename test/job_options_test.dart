import 'dart:async';

import 'package:dox_queue/dox_queue.dart';
import 'package:test/test.dart';

import 'fixtures/jobs.g.dart';
import 'fixtures/test_jobs.dart';
import 'support.dart';

/// a job with the opt-in options. It throws, or waits past its timeout.
class OptionsJob extends DoxJob {
  OptionsJob({
    required this.key,
    this.maxTries = 1,
    this.untilMillis,
    this.exceptions,
    this.onTimeout = false,
    this.slow = false,
  });

  final String key;
  final int maxTries;
  final int? untilMillis;
  final int? exceptions;
  final bool onTimeout;
  final bool slow;

  @override
  int get tries => maxTries;

  @override
  DateTime? get retryUntil {
    int? millis = untilMillis;
    return millis == null
        ? null
        : DateTime.now().add(Duration(milliseconds: millis));
  }

  @override
  int? get maxExceptions => exceptions;

  @override
  bool get failOnTimeout => onTimeout;

  @override
  Duration? get timeout => const Duration(milliseconds: 50);

  @override
  List<Duration> get backoff => const <Duration>[Duration(milliseconds: 50)];

  @override
  Future<void> handle() async {
    JobLog.runs[key] = (JobLog.runs[key] ?? 0) + 1;
    if (slow) {
      await Future<void>.delayed(const Duration(milliseconds: 150));
      return;
    }
    throw StateError('options $key');
  }

  @override
  void failed(Object error, StackTrace stack) {
    JobLog.failed.add(key);
  }
}

Map<String, Object?> _encode(DoxJob job) {
  OptionsJob value = job as OptionsJob;
  return <String, Object?>{
    'key': value.key,
    'maxTries': value.maxTries,
    'untilMillis': value.untilMillis,
    'exceptions': value.exceptions,
    'onTimeout': value.onTimeout,
    'slow': value.slow,
  };
}

DoxJob _decode(Map<String, Object?> data) {
  return OptionsJob(
    key: data['key'] as String,
    maxTries: data['maxTries'] as int,
    untilMillis: data['untilMillis'] as int?,
    exceptions: data['exceptions'] as int?,
    onTimeout: data['onTimeout'] as bool,
    slow: data['slow'] as bool,
  );
}

JobRegistry _jobs = JobRegistry(<JobCodec>[
  ...jobRegistry.codecs,
  const JobCodec('OptionsJob', OptionsJob, _encode, _decode),
]);

Future<void> _wait(int millis) {
  return Future<void>.delayed(Duration(milliseconds: millis));
}

void main() {
  setUpAll(setUpTestApp);

  test('a payload of an old row has no retryUntil and no exceptions', () {
    JobPayload old = JobPayload.decode(
      '{"id":"a","job":"J","queue":"default","data":"{}","attempts":2}',
    );
    expect(old.retryUntil, isNull);
    expect(old.exceptions, 0);
    expect(old.attempts, 2);
    // a job with neither is saved as before
    expect(old.encode(), isNot(contains('retryUntil')));
    expect(old.encode(), isNot(contains('exceptions')));
    JobPayload next = JobPayload.decode(
      JobPayload(
        id: 'b',
        job: 'J',
        queue: 'q',
        data: '{}',
        retryUntil: 99,
      ).withExceptions(3).withAttempts(1).encode(),
    );
    expect(next.retryUntil, 99);
    expect(next.exceptions, 3);
    expect(next.attempts, 1);
  });

  test('Worker.backoffFor uses the worker backoff for a job with none', () {
    SendWelcomeEmail job = SendWelcomeEmail(userId: 1);
    List<Duration> fallback = const <Duration>[
      Duration(seconds: 1),
      Duration(seconds: 5),
    ];
    expect(Worker.backoffFor(job, 1), Duration.zero);
    expect(Worker.backoffFor(job, 1, fallback), Duration(seconds: 1));
    expect(Worker.backoffFor(job, 9, fallback), Duration(seconds: 5));
    FlakyJob flaky = FlakyJob(key: 'a', failTimes: 1);
    expect(Worker.backoffFor(flaky, 1, fallback), flaky.backoff.first);
  });

  group('fake', () {
    late QueueFake queue;

    setUp(() => queue = Queue.fake());

    tearDown(Queue.restore);

    test('dispatchIf and dispatchUnless', () async {
      expect(await SendWelcomeEmail(userId: 1).dispatchIf(false), isNull);
      expect(await SendWelcomeEmail(userId: 2).dispatchUnless(true), isNull);
      queue.assertNothingPushed();
      expect(await SendWelcomeEmail(userId: 3).dispatchIf(true), isNotNull);
      expect(
        await SendWelcomeEmail(userId: 4)
            .dispatchUnless(false, queue: 'emails'),
        isNotNull,
      );
      expect(queue.jobsOf<SendWelcomeEmail>().map(_userId), <int>[3, 4]);
      expect(queue.pushed.last.queue, 'emails');
    });

    test('delayUntil turns into a delay', () async {
      await SendWelcomeEmail(userId: 1)
          .dispatch(delayUntil: DateTime.now().add(Duration(minutes: 10)));
      Duration delay = queue.pushed.single.delay!;
      expect(delay, greaterThan(Duration(minutes: 9)));
      expect(delay, lessThanOrEqualTo(Duration(minutes: 10)));
      // a past time: at once
      await SendWelcomeEmail(userId: 2)
          .dispatch(delayUntil: DateTime.now().subtract(Duration(minutes: 1)));
      expect(queue.pushed.last.delay, Duration.zero);
      expect(
        () =>
            SendWelcomeEmail(userId: 3)
                .dispatch(delay: Duration.zero, delayUntil: DateTime.now()),
        throwsArgumentError,
      );
    });
  });

  for (TestStore store in testStores) {
    group('${store.name} job options', () {
      setUp(() async {
        await store.reset();
        JobLog.clear();
        Queue.configure(
          store.config(
            jobs: _jobs,
            retryAfter: const Duration(milliseconds: 500),
          ),
        );
      });

      tearDown(Queue.close);

      Worker worker([WorkerOptions? options]) {
        return Worker(
          driver: Queue.driver,
          jobs: _jobs,
          options: options ?? const WorkerOptions(stopWhenEmpty: true),
        );
      }

      /// work until [key] failed, or 5 seconds
      Future<void> workUntilFailed(String key) async {
        Stopwatch watch = Stopwatch()..start();
        while (!JobLog.failed.contains(key) && watch.elapsed.inSeconds < 5) {
          await worker().run();
          await _wait(20);
        }
      }

      test('delayUntil waits until the time', () async {
        await SendWelcomeEmail(userId: 1).dispatch(
          delayUntil: DateTime.now().add(const Duration(milliseconds: 300)),
        );
        expect(await Queue.driver.pop('default'), isNull);
        await _wait(400);
        expect(await Queue.driver.pop('default'), isNotNull);
      });

      test('maxExceptions fails before the tries are used', () async {
        await OptionsJob(key: 'max', maxTries: 10, exceptions: 2).dispatch();
        await workUntilFailed('max');
        expect(JobLog.runs['max'], 2);
        List<FailedJob> failed = await Queue.driver.failed();
        expect(JobPayload.decode(failed.single.payload).exceptions, 2);
      });

      test('retryUntil retries past tries until the time', () async {
        await OptionsJob(key: 'until', untilMillis: 400).dispatch();
        await workUntilFailed('until');
        // tries is 1, but it ran until the time passed
        expect(JobLog.runs['until'], greaterThan(1));
        expect(JobLog.failed, <String>['until']);
      });

      test('a retried job gets its full maxExceptions again', () async {
        await OptionsJob(key: 'again', maxTries: 10, exceptions: 2).dispatch();
        await workUntilFailed('again');
        expect(JobLog.runs['again'], 2);
        JobLog.failed.clear();
        String id = (await Queue.driver.failed()).single.id;
        expect(await Queue.driver.retry(id), isTrue);
        await workUntilFailed('again');
        // two more tries, not one: the exceptions count starts at 0
        expect(JobLog.runs['again'], 4);
        List<FailedJob> failed = await Queue.driver.failed();
        expect(JobPayload.decode(failed.single.payload).exceptions, 2);
      });

      test('a retried job gets a new retryUntil window', () async {
        await OptionsJob(key: 'window', untilMillis: 400).dispatch();
        await workUntilFailed('window');
        int first = JobLog.runs['window']!;
        expect(first, greaterThan(1));
        JobLog.failed.clear();
        String id = (await Queue.driver.failed()).single.id;
        // the old window is over
        await _wait(100);
        expect(await Queue.driver.retry(id), isTrue);
        await workUntilFailed('window');
        // more than one try: the old window would give only one
        expect(JobLog.runs['window']! - first, greaterThan(1));
      });

      test('without retryUntil, tries is the limit', () async {
        await OptionsJob(key: 'once').dispatch();
        await workUntilFailed('once');
        expect(JobLog.runs['once'], 1);
      });

      test('failOnTimeout fails at once on a timeout', () async {
        await OptionsJob(
          key: 'slow',
          maxTries: 3,
          onTimeout: true,
          slow: true,
        ).dispatch();
        await workUntilFailed('slow');
        expect(JobLog.runs['slow'], 1);
        List<FailedJob> failed = await Queue.driver.failed();
        expect(failed.single.error, contains('timed out'));
      });

      test('a timeout is not an exception for maxExceptions', () async {
        await OptionsJob(
          key: 'slow2',
          maxTries: 2,
          exceptions: 1,
          slow: true,
        ).dispatch();
        await workUntilFailed('slow2');
        // maxExceptions 1 did not stop it: both tries ran
        expect(JobLog.runs['slow2'], 2);
      });

      test('--max-jobs stops the worker', () async {
        for (int i = 1; i <= 3; i++) {
          await SendWelcomeEmail(userId: i).dispatch();
        }
        await worker(const WorkerOptions(maxJobs: 2)).run();
        expect(JobLog.handled, <String>['welcome 1', 'welcome 2']);
        expect(await Queue.driver.pop('default'), isNotNull);
      });

      test('--max-time stops an idle worker', () async {
        Stopwatch watch = Stopwatch()..start();
        await worker(const WorkerOptions(maxTime: Duration(milliseconds: 200)))
            .run()
            .timeout(const Duration(seconds: 5));
        expect(watch.elapsedMilliseconds, lessThan(2000));
      });

      test('--memory stops the worker after a job', () async {
        await SendWelcomeEmail(userId: 1).dispatch();
        await SendWelcomeEmail(userId: 2).dispatch();
        // 1 MB: every process uses more
        await worker(const WorkerOptions(memory: 1))
            .run()
            .timeout(const Duration(seconds: 5));
        expect(JobLog.handled, <String>['welcome 1']);
      });
    }, skip: store.skip);
  }
}

int _userId(SendWelcomeEmail job) => job.userId;
