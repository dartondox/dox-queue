import 'dart:async';

import 'package:dox_queue/dox_queue.dart';
import 'package:test/test.dart';

import 'fixtures/jobs.g.dart';
import 'fixtures/test_jobs.dart';

const Duration _grace = Duration(milliseconds: 300);

/// pop() never returns, like a Redis client that retries for ever
class _HangingDriver implements QueueDriver {
  int pops = 0;

  @override
  Future<ReservedJob?> pop(String queue) {
    pops++;
    return Completer<ReservedJob?>().future;
  }

  @override
  dynamic noSuchMethod(Invocation invocation) => super.noSuchMethod(invocation);
}

/// pop() returns one job after a delay, then nothing
class _SlowDriver implements QueueDriver {
  _SlowDriver(this.delay);

  final Duration delay;
  bool given = false;
  int pops = 0;
  int deleted = 0;
  final List<ReservedJob> released = <ReservedJob>[];

  @override
  Future<ReservedJob?> pop(String queue) async {
    pops++;
    await Future<void>.delayed(delay);
    if (given) return null;
    given = true;
    JobPayload payload = JobPayload(
      id: 'a' * 32,
      job: 'SendWelcomeEmail',
      queue: queue,
      data: '{"userId":5}',
    );
    return ReservedJob(payload, attempts: 1, ref: 1);
  }

  @override
  Future<void> delete(ReservedJob job) async {
    deleted++;
  }

  @override
  Future<void> release(ReservedJob job, Duration delay) async {
    released.add(job);
  }

  @override
  dynamic noSuchMethod(Invocation invocation) => super.noSuchMethod(invocation);
}

/// pop() returns null after a delay
class _EmptyDriver implements QueueDriver {
  _EmptyDriver(this.delay);

  final Duration delay;
  int pops = 0;

  @override
  Future<ReservedJob?> pop(String queue) async {
    pops++;
    await Future<void>.delayed(delay);
    return null;
  }

  @override
  dynamic noSuchMethod(Invocation invocation) => super.noSuchMethod(invocation);
}

class _StopOnLoop {
  _StopOnLoop(this.worker);

  final Worker worker;

  void call(QueueEvent event) {
    worker.stop();
  }
}

void main() {
  tearDown(Queue.forgetListeners);

  test('stop() ends run() while pop is stuck', () async {
    Worker worker = Worker(
      driver: _HangingDriver(),
      jobs: jobRegistry,
      popStopGrace: _grace,
    );
    Future<void> running = worker.run();
    await Future<void>.delayed(const Duration(milliseconds: 50));
    Stopwatch watch = Stopwatch()..start();
    worker.stop();
    await running.timeout(const Duration(seconds: 3));
    // run() waited for the grace, it did not end at once
    expect(watch.elapsedMilliseconds, greaterThanOrEqualTo(250));
  });

  test('stop() ends run() with concurrency 3 and stuck pops', () async {
    _HangingDriver driver = _HangingDriver();
    Worker worker = Worker(
      driver: driver,
      jobs: jobRegistry,
      options: const WorkerOptions(concurrency: 3),
      popStopGrace: _grace,
    );
    Future<void> running = worker.run();
    await Future<void>.delayed(const Duration(milliseconds: 50));
    expect(driver.pops, 3);
    worker.stop();
    await running.timeout(const Duration(seconds: 2));
  });

  test('a pop that returns a job within the grace still runs it', () async {
    JobLog.clear();
    _SlowDriver driver = _SlowDriver(const Duration(milliseconds: 150));
    Worker worker = Worker(
      driver: driver,
      jobs: jobRegistry,
      popStopGrace: const Duration(seconds: 3),
    );
    Future<void> running = worker.run();
    await Future<void>.delayed(const Duration(milliseconds: 30));
    worker.stop();
    await running.timeout(const Duration(seconds: 5));
    expect(JobLog.handled, <String>['welcome 5']);
    expect(driver.deleted, 1);
    expect(driver.released, isEmpty);
  });

  test('a pop that returns a job after the grace gives it back', () async {
    JobLog.clear();
    _SlowDriver driver = _SlowDriver(const Duration(milliseconds: 900));
    Worker worker = Worker(
      driver: driver,
      jobs: jobRegistry,
      popStopGrace: _grace,
    );
    Future<void> running = worker.run();
    await Future<void>.delayed(const Duration(milliseconds: 30));
    worker.stop();
    await running.timeout(const Duration(seconds: 3));
    expect(driver.released, isEmpty);
    await Future<void>.delayed(const Duration(milliseconds: 1000));
    expect(driver.released.length, 1);
    expect(driver.released.single.payload.job, 'SendWelcomeEmail');
    expect(JobLog.handled, isEmpty);
    expect(driver.deleted, 0);
  });

  test('no new pop starts after stop()', () async {
    _EmptyDriver driver = _EmptyDriver(const Duration(milliseconds: 300));
    Worker worker = Worker(
      driver: driver,
      jobs: jobRegistry,
      options: const WorkerOptions(queues: <String>['a', 'b', 'c']),
      popStopGrace: const Duration(seconds: 3),
    );
    Future<void> running = worker.run();
    await Future<void>.delayed(const Duration(milliseconds: 50));
    worker.stop();
    await running.timeout(const Duration(seconds: 5));
    await Future<void>.delayed(const Duration(milliseconds: 400));
    // only the pop of queue a ran. The queues b and c were never popped.
    expect(driver.pops, 1);
  });

  test('a looping listener that calls stop() starts no pop', () async {
    _HangingDriver driver = _HangingDriver();
    Worker worker = Worker(
      driver: driver,
      jobs: jobRegistry,
      popStopGrace: _grace,
    );
    QueueEvent.listen(QueueEventType.looping, _StopOnLoop(worker).call);
    await worker.run().timeout(const Duration(seconds: 3));
    expect(driver.pops, 0);
  });

  test('stop() after run() ended is harmless and starts no timer', () async {
    _EmptyDriver driver = _EmptyDriver(const Duration(milliseconds: 10));
    Worker worker = Worker(
      driver: driver,
      jobs: jobRegistry,
      options: const WorkerOptions(stopWhenEmpty: true),
      popStopGrace: const Duration(milliseconds: 50),
    );
    await worker.run();
    worker.stop();
    worker.stop();
  });
}
