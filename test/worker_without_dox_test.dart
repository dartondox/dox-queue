import 'package:dox_queue/dox_queue.dart';
import 'package:test/test.dart';

import 'fixtures/jobs.g.dart';

/// pop() throws once, then the queue is empty
class _BrokenDriver implements QueueDriver {
  int calls = 0;

  @override
  Future<ReservedJob?> pop(String queue) async {
    calls++;
    if (calls == 1) throw StateError('db is down');
    return null;
  }

  @override
  dynamic noSuchMethod(Invocation invocation) => super.noSuchMethod(invocation);
}

// no Dox().initialize() in this file on purpose
void main() {
  test('a worker logs without Dox().initialize()', () async {
    _BrokenDriver driver = _BrokenDriver();
    await Worker(
      driver: driver,
      jobs: jobRegistry,
      options: const WorkerOptions(
        stopWhenEmpty: true,
        sleep: Duration(milliseconds: 1),
      ),
    ).run();
    expect(driver.calls, 2);
  });
}
