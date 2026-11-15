import 'dart:async';

import 'package:dox_queue/dox_queue.dart';
import 'package:test/test.dart';

import 'fixtures/jobs.g.dart';

/// A driver with only `pop()`: no [MultiQueuePop]. It notes each pop,
/// and gives one job at pop number [jobAt].
class _PlainDriver implements QueueDriver {
  _PlainDriver({this.jobAt = 0});

  final int jobAt;
  final Stopwatch clock = Stopwatch()..start();

  /// the time of each pop, in milliseconds
  final List<int> times = <int>[];

  /// the queue of each pop
  final List<String> queues = <String>[];

  @override
  Future<ReservedJob?> pop(String queue) async {
    queues.add(queue);
    times.add(clock.elapsedMilliseconds);
    if (times.length != jobAt) return null;
    JobPayload payload = JobPayload(
      id: 'a',
      job: 'SendWelcomeEmail',
      queue: queue,
      data: '{"userId":1}',
    );
    return ReservedJob(payload, attempts: 1, ref: 1);
  }

  @override
  Future<void> delete(ReservedJob job) async {}

  @override
  dynamic noSuchMethod(Invocation invocation) => super.noSuchMethod(invocation);
}

/// run [worker] for [millis], then stop it
Future<void> _runFor(Worker worker, int millis) async {
  Future<void> running = worker.run();
  await Future<void>.delayed(Duration(milliseconds: millis));
  worker.stop();
  await running;
}

void main() {
  test('a driver with only pop() is asked queue by queue', () async {
    _PlainDriver driver = _PlainDriver();
    await Worker(
      driver: driver,
      jobs: jobRegistry,
      options: const WorkerOptions(
        queues: <String>['high', 'default'],
        stopWhenEmpty: true,
      ),
    ).run();
    expect(driver.queues, <String>['high', 'default']);
  });

  test('the sleep stays the same without maxSleep', () async {
    _PlainDriver driver = _PlainDriver();
    await _runFor(
      Worker(
        driver: driver,
        jobs: jobRegistry,
        options: const WorkerOptions(sleep: Duration(milliseconds: 20)),
      ),
      600,
    );
    // about 30 pops; fewer on a slow machine
    expect(driver.times.length, greaterThan(15));
  });

  test('maxSleep doubles the sleep when the queues stay empty', () async {
    _PlainDriver driver = _PlainDriver();
    await _runFor(
      Worker(
        driver: driver,
        jobs: jobRegistry,
        options: const WorkerOptions(
          sleep: Duration(milliseconds: 20),
          maxSleep: Duration(milliseconds: 80),
        ),
      ),
      600,
    );
    // waits of 20, 40, 80, 80, ...: about 9 pops
    expect(driver.times.length, lessThan(13));
    List<int> times = driver.times;
    expect(times[2] - times[1], greaterThanOrEqualTo(35));
    expect(times.last - times[times.length - 2], greaterThanOrEqualTo(75));
  });

  test('a job resets the sleep to the start', () async {
    // pops 1 to 3 are empty (waits of 20, 40, 80), pop 4 gives a job,
    // pop 5 comes at once and is empty, then the wait is 20 again
    _PlainDriver driver = _PlainDriver(jobAt: 4);
    Worker worker = Worker(
      driver: driver,
      jobs: jobRegistry,
      options: const WorkerOptions(
        sleep: Duration(milliseconds: 20),
        maxSleep: Duration(milliseconds: 160),
      ),
    );
    Future<void> running = worker.run();
    while (driver.times.length < 6) {
      await Future<void>.delayed(const Duration(milliseconds: 5));
    }
    worker.stop();
    await running;
    List<int> times = driver.times;
    expect(times[3] - times[2], greaterThanOrEqualTo(75));
    expect(times[5] - times[4], lessThan(times[3] - times[2]));
  });

  test('QueueEvent.hasListeners', () {
    expect(QueueEvent.hasListeners(QueueEventType.processed), isFalse);
    Queue.after(_ignore);
    expect(QueueEvent.hasListeners(QueueEventType.processed), isTrue);
    expect(QueueEvent.hasListeners(QueueEventType.failed), isFalse);
    Queue.forgetListeners();
    expect(QueueEvent.hasListeners(QueueEventType.processed), isFalse);
  });

  test('newId gives 32 hex characters, a new one each time', () {
    Set<String> ids = <String>{
      for (int i = 0; i < 1000; i++) JobPayload.newId(),
    };
    expect(ids, hasLength(1000));
    for (String id in ids) {
      expect(id, matches(RegExp(r'^[0-9a-f]{32}$')));
    }
  });
}

void _ignore(QueueEvent event) {}
