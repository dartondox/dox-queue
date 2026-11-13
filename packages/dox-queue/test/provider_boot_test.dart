import 'package:dox_core/dox_core.dart' show AppConfig, Dox;
import 'package:dox_queue/dox_queue.dart';
import 'package:test/test.dart';

/// an empty queue that counts the pops of the worker
class _EmptyDriver implements QueueDriver {
  int pops = 0;

  @override
  Future<ReservedJob?> pop(String queue) async {
    pops++;
    return null;
  }

  @override
  Future<void> close() async {}

  @override
  dynamic noSuchMethod(Invocation invocation) => super.noSuchMethod(invocation);
}

class _Factory implements QueueDriverFactory {
  final _EmptyDriver driver = _EmptyDriver();

  @override
  QueueDriver create(QueueConfig config, {String? name}) => driver;
}

void main() {
  test('the in-process worker starts in boot(), not in setup()', () async {
    Dox().initialize(AppConfig(appKey: 'k' * 32, totalIsolate: 1));
    _Factory factory = _Factory();
    Queue.extend('empty', factory);
    QueueProvider provider = QueueProvider(
      QueueConfig(driver: 'empty', workInProcess: true),
    );
    await provider.setup();
    // other providers (eg. the mail jobs) are set up here
    await Future<void>.delayed(const Duration(milliseconds: 50));
    expect(factory.driver.pops, 0);

    await provider.boot();
    await Future<void>.delayed(const Duration(milliseconds: 50));
    expect(factory.driver.pops, greaterThan(0));
    await provider.shutdown();
  });
}
