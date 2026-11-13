import 'package:dox_queue/dox_queue.dart';
import 'package:test/test.dart';

import 'fixtures/jobs.g.dart';
import 'fixtures/test_jobs.dart';

class _NullDriver implements QueueDriver {
  @override
  dynamic noSuchMethod(Invocation invocation) => super.noSuchMethod(invocation);
}

/// a driver that only counts how often it was made
class _Factory implements QueueDriverFactory {
  final List<String?> made = <String?>[];

  @override
  QueueDriver create(QueueConfig config, {String? name}) {
    made.add(name);
    return _NullDriver();
  }
}

class _Transactions implements QueueTransactions {
  bool open = true;
  final List<Future<void> Function()> waiting = <Future<void> Function()>[];

  @override
  bool get inTransaction => open;

  @override
  Future<void> afterCommit(Future<void> Function() callback) async {
    waiting.add(callback);
  }
}

void main() {
  setUp(JobLog.clear);

  tearDown(() {
    Queue.useTransactions(null);
    Queue.useBatches(null);
  });

  test('Queue.extend adds a driver', () {
    _Factory factory = _Factory();
    Queue.extend('memo', factory);
    QueueConfig config = QueueConfig(
      driver: 'memo',
      connections: <String, QueueConfig>{'other': QueueConfig(driver: 'memo')},
    );
    config.createDriver();
    config.connectionOf('other').createDriver(name: 'other');
    expect(factory.made, <String?>[null, 'other']);
  });

  test('the database driver without dox_queue_database says what to add', () {
    expect(
      () => QueueConfig(driver: 'database').createDriver(),
      throwsA(
        isA<ArgumentError>().having(
          (ArgumentError e) => '${e.message}',
          'message',
          contains('dox_queue_database'),
        ),
      ),
    );
  });

  test('a batch without a batch store throws', () async {
    Queue.configure(QueueConfig(jobs: jobRegistry));
    expect(
      () => Queue.batch(<DoxJob>[SendWelcomeEmail(userId: 1)]).dispatch(),
      throwsA(isA<StateError>()),
    );
    expect(JobLog.handled, isEmpty);
  });

  test('pruneBatches without a batch store gives a failed Future', () {
    Queue.configure(QueueConfig(jobs: jobRegistry));
    Future<int> result = Queue.pruneBatches(DateTime.now());
    expect(result, throwsA(isA<StateError>()));
  });

  test('afterCommit without transactions throws', () async {
    Queue.configure(QueueConfig(jobs: jobRegistry));
    expect(
      () => SendWelcomeEmail(userId: 1).dispatch(afterCommit: true),
      throwsA(isA<StateError>()),
    );
    // off: no transactions needed
    await SendWelcomeEmail(userId: 2).dispatch();
    expect(JobLog.handled, <String>['welcome 2']);
  });

  test('afterCommit waits for the transactions of useTransactions', () async {
    Queue.configure(QueueConfig(jobs: jobRegistry));
    _Transactions transactions = _Transactions();
    Queue.useTransactions(transactions);
    await SendWelcomeEmail(userId: 3).dispatch(afterCommit: true);
    expect(JobLog.handled, isEmpty);
    await transactions.waiting.single();
    expect(JobLog.handled, <String>['welcome 3']);

    transactions.open = false;
    await SendWelcomeEmail(userId: 4).dispatch(afterCommit: true);
    expect(JobLog.handled, <String>['welcome 3', 'welcome 4']);
  });

  test('JobRegistry.addBuiltIn adds a job to every registry', () {
    const JobCodec codec = JobCodec(
      'dox_queue:test',
      SendWelcomeEmail,
      _encode,
      _decode,
    );
    JobRegistry.addBuiltIn(codec);
    JobRegistry.addBuiltIn(codec);
    expect(
      JobRegistry.builtIn.where((JobCodec c) => c.name == 'dox_queue:test'),
      hasLength(1),
    );
    expect(const JobRegistry(<JobCodec>[]).byName('dox_queue:test'), codec);
  });
}

Map<String, Object?> _encode(DoxJob job) => <String, Object?>{};

DoxJob _decode(Map<String, Object?> data) => SendWelcomeEmail(userId: 0);
