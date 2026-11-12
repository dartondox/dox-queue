import 'dart:async';

import 'package:dox_queue/dox_queue.dart';
import 'package:test/test.dart';

import 'fixtures/jobs.g.dart';

/// pop() never returns, like a Redis client that retries for ever
class _HangingDriver implements QueueDriver {
  @override
  Future<ReservedJob?> pop(String queue) {
    return Completer<ReservedJob?>().future;
  }

  @override
  dynamic noSuchMethod(Invocation invocation) => super.noSuchMethod(invocation);
}

void main() {
  test('stop() ends run() while pop is stuck', () async {
    Worker worker = Worker(driver: _HangingDriver(), jobs: jobRegistry);
    Future<void> running = worker.run();
    await Future<void>.delayed(const Duration(milliseconds: 50));
    worker.stop();
    await running.timeout(const Duration(seconds: 2));
  });
}
