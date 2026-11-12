import 'dart:async';

import 'package:dox_queue/dox_queue.dart';
import 'package:test/test.dart';

import 'fixtures/jobs.g.dart';
import 'fixtures/test_jobs.dart';

const Duration _grace = Duration(milliseconds: 200);

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
  dynamic noSuchMethod(Invocation invocation) => super.noSuchMethod(invocation);
}

void main() {
  test('stop() ends run() while pop is stuck', () async {
    Worker worker = Worker(
      driver: _HangingDriver(),
      jobs: jobRegistry,
      popStopGrace: _grace,
    );
    Future<void> running = worker.run();
    await Future<void>.delayed(const Duration(milliseconds: 50));
    worker.stop();
    await running.timeout(const Duration(seconds: 2));
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
    _SlowDriver driver = _SlowDriver(const Duration(milliseconds: 100));
    Worker worker = Worker(
      driver: driver,
      jobs: jobRegistry,
      popStopGrace: const Duration(seconds: 2),
    );
    Future<void> running = worker.run();
    await Future<void>.delayed(const Duration(milliseconds: 30));
    worker.stop();
    await running.timeout(const Duration(seconds: 5));
    expect(JobLog.handled, <String>['welcome 5']);
    expect(driver.deleted, 1);
  });

  test('no new pop starts after stop()', () async {
    _SlowDriver driver = _SlowDriver(const Duration(milliseconds: 100));
    Worker worker = Worker(
      driver: driver,
      jobs: jobRegistry,
      options: const WorkerOptions(queues: <String>['a', 'b', 'c']),
      popStopGrace: const Duration(seconds: 2),
    );
    Future<void> running = worker.run();
    await Future<void>.delayed(const Duration(milliseconds: 30));
    worker.stop();
    await running.timeout(const Duration(seconds: 5));
    await Future<void>.delayed(const Duration(milliseconds: 300));
    // the first pop gave a job. The queues b and c were never popped.
    expect(driver.pops, 1);
  });
}
