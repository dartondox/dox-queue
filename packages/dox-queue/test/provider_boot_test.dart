import 'package:dox_core/dox_core.dart' show AppConfig, Dox, DoxProvider, Env;
import 'package:dox_queue/dox_queue.dart';
import 'package:test/test.dart';

/// an empty queue that counts the pops of the worker
class _EmptyDriver implements QueueDriver {
  int pops = 0;

  @override
  Future<List<FailedJob>> failed() async => <FailedJob>[];

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
  int made = 0;

  @override
  QueueDriver create(QueueConfig config, {String? name}) {
    made++;
    return driver;
  }
}

/// what the providers did, in order
List<String> _steps = <String>[];

class _LogProvider extends DoxProvider {
  _LogProvider(this.name);

  final String name;

  @override
  void setup() => _steps.add('$name setup');

  @override
  Future<void> boot() async {
    await Future<void>.delayed(const Duration(milliseconds: 5));
    _steps.add('$name boot');
  }
}

class _OtherLogProvider extends _LogProvider {
  _OtherLogProvider() : super('b');
}

void main() {
  test('the in-process worker starts in boot(), not in setup()', () async {
    Dox().initialize(AppConfig(appKey: 'k' * 32, totalIsolate: 1));
    _Factory factory = _Factory();
    Queue.extend('empty', factory);
    QueueProvider provider = QueueProvider(
      QueueConfig(driver: 'empty', inProcess: true),
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

  test('bin/queue.dart runs boot() after every setup(), and no second '
      'worker', () async {
    _Factory factory = _Factory();
    Queue.extend('empty', factory);
    AppConfig app = AppConfig(
      appKey: 'k' * 32,
      totalIsolate: 1,
      providers: <DoxProvider>[
        _LogProvider('a'),
        QueueProvider(QueueConfig(driver: 'empty', inProcess: true)),
        _OtherLogProvider(),
      ],
    );
    _steps.clear();
    expect(await QueueWorker(app: app).run(<String>['failed']), 0);
    expect(_steps, <String>['a setup', 'b setup', 'a boot', 'b boot']);
    // only the store of the `failed` command: QueueWorker.active kept the
    // in-process worker of QueueProvider.boot() off
    expect(factory.made, 1);
    expect(factory.driver.pops, 0);
  });

  test('the worker stops on a short APP_KEY in production', () async {
    Queue.extend('empty', _Factory());
    Env().env['APP_ENV'] = 'production';
    addTearDown(() => Env().env.remove('APP_ENV'));
    AppConfig app = AppConfig(
      appKey: 'short',
      totalIsolate: 1,
      providers: <DoxProvider>[QueueProvider(QueueConfig(driver: 'empty'))],
    );
    expect(await QueueWorker(app: app).run(<String>['work', '--once']), 1);
  });

  test('the worker only warns on a short APP_KEY in development', () async {
    Queue.extend('empty', _Factory());
    Env().env['APP_ENV'] = 'development';
    addTearDown(() => Env().env.remove('APP_ENV'));
    AppConfig app = AppConfig(
      appKey: 'short',
      totalIsolate: 1,
      providers: <DoxProvider>[QueueProvider(QueueConfig(driver: 'empty'))],
    );
    expect(
      await QueueWorker(app: app).run(<String>['work', '--stop-when-empty']),
      0,
    );
  });

  test('inProcess with the database driver but no dox_queue_database '
      'fails the start with a clear error', () async {
    Dox().initialize(AppConfig(appKey: 'k' * 32, totalIsolate: 1));
    QueueProvider provider = QueueProvider(
      QueueConfig(driver: 'database', inProcess: true),
    );
    await provider.setup();
    await expectLater(
      provider.boot(),
      throwsA(
        isA<ArgumentError>().having(
          (ArgumentError e) => '${e.message}',
          'message',
          contains('dox_queue_database'),
        ),
      ),
    );
  });
}
