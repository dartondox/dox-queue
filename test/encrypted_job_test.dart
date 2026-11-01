import 'package:dox_core/dox_core.dart' show AppConfig, DecryptException, Dox;
import 'package:dox_queue/dox_queue.dart';
import 'package:test/test.dart';

import 'support.dart';

List<String> charged = <String>[];

class ChargeCard extends DoxJob with ShouldBeEncrypted {
  ChargeCard({required this.card});

  final String card;

  @override
  Future<void> handle() async {
    charged.add(card);
  }
}

class PlainJob extends DoxJob {
  PlainJob({required this.card});

  final String card;

  @override
  Future<void> handle() async {
    charged.add(card);
  }
}

Map<String, Object?> _encode(DoxJob job) {
  return <String, Object?>{
    'card': job is ChargeCard ? job.card : (job as PlainJob).card,
  };
}

DoxJob _decodeCharge(Map<String, Object?> data) {
  return ChargeCard(card: data['card'] as String);
}

DoxJob _decodePlain(Map<String, Object?> data) {
  return PlainJob(card: data['card'] as String);
}

JobRegistry registry = JobRegistry(<JobCodec>[
  JobCodec('ChargeCard', ChargeCard, _encode, _decodeCharge),
  JobCodec('PlainJob', PlainJob, _encode, _decodePlain),
]);

void main() {
  setUpAll(setUpTestApp);

  TestStore store = testStores.first;

  setUp(() async {
    await store.reset();
    charged.clear();
    Queue.configure(store.config(jobs: registry));
  });

  tearDown(Queue.close);

  /// take the job, return its stored data, then run it
  Future<String> takeAndRun() async {
    ReservedJob? job = await Queue.driver.pop('default');
    await Worker(driver: Queue.driver, jobs: registry).process(job!);
    return job.payload.data;
  }

  test('a ShouldBeEncrypted job is saved encrypted and still runs', () async {
    await ChargeCard(card: '4242-secret').dispatch();
    String data = await takeAndRun();
    expect(data, isNot(contains('4242-secret')));
    expect(data.startsWith('{'), isFalse);
    expect(Queue.readData(data, 'ChargeCard'), '{"card":"4242-secret"}');
    expect(charged, <String>['4242-secret']);
    expect(await Queue.driver.failed(), isEmpty);
  });

  test('a job without the mixin is saved as plain JSON, as before', () async {
    await PlainJob(card: 'visible').dispatch();
    expect(await takeAndRun(), '{"card":"visible"}');
    expect(charged, <String>['visible']);
  });

  test('encrypted data can not be moved to another job', () {
    String data = Queue.encryptData('{"card":"x"}', 'ChargeCard');
    expect(
      () => Queue.readData(data, 'PlainJob'),
      throwsA(isA<DecryptException>()),
    );
  });

  group('key rotation', () {
    String oldKey = 'k' * 32;
    String newKey = 'n' * 32;

    tearDown(setUpTestApp);

    test('data made with an old key is read with previousKeys', () {
      String data = Queue.encryptData('{"card":"x"}', 'ChargeCard');
      Dox().initialize(
        AppConfig(appKey: newKey, previousKeys: <String>[oldKey]),
      );
      expect(Queue.readData(data, 'ChargeCard'), '{"card":"x"}');
    });

    test('data made with another key throws DecryptException', () {
      String data = Queue.encryptData('{"card":"x"}', 'ChargeCard');
      Dox().initialize(AppConfig(appKey: newKey));
      expect(
        () => Queue.readData(data, 'ChargeCard'),
        throwsA(isA<DecryptException>()),
      );
    });

    test('the worker fails a job with a wrong key, with no retry', () async {
      await ChargeCard(card: 'x').dispatch();
      Dox().initialize(AppConfig(appKey: newKey));
      ReservedJob? job = await Queue.driver.pop('default');
      await Worker(driver: Queue.driver, jobs: registry).process(job!);
      expect(charged, isEmpty);
      List<FailedJob> failed = await Queue.driver.failed();
      expect(failed.single.error, contains('DecryptException'));
      expect(failed.single.error, isNot(contains('"card"')));
    });
  });
}
