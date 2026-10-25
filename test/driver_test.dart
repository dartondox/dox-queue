import 'package:dox_queue/dox_queue.dart';
import 'package:test/test.dart';

import 'support.dart';

JobPayload _payload(String id, {String queue = 'default'}) {
  return JobPayload(id: id, job: 'SendWelcomeEmail', queue: queue, data: '{}');
}

Future<void> _wait(int millis) {
  return Future<void>.delayed(Duration(milliseconds: millis));
}

void main() {
  setUpAll(setUpTestApp);

  for (TestStore store in testStores) {
    group('${store.name} driver', () {
      late QueueDriver driver;

      setUp(() async {
        await store.reset();
        driver = store
            .config(retryAfter: const Duration(milliseconds: 400))
            .createDriver();
      });

      tearDown(() async {
        await driver.close();
      });

      test('push, pop and delete', () async {
        await driver.push(_payload('a'));
        await driver.push(_payload('b'));
        ReservedJob? first = await driver.pop('default');
        expect(first!.payload.id, 'a');
        expect(first.attempts, 1);
        expect(first.payload.data, '{}');
        await driver.delete(first);
        ReservedJob? second = await driver.pop('default');
        expect(second!.payload.id, 'b');
        await driver.delete(second);
        expect(await driver.pop('default'), isNull);
      });

      test('queues are apart', () async {
        await driver.push(_payload('a', queue: 'emails'));
        expect(await driver.pop('default'), isNull);
        expect((await driver.pop('emails'))!.payload.id, 'a');
      });

      test('a delayed job comes after its delay', () async {
        await driver.push(
          _payload('a'),
          delay: const Duration(milliseconds: 300),
        );
        expect(await driver.pop('default'), isNull);
        await _wait(400);
        expect((await driver.pop('default'))!.payload.id, 'a');
      });

      test('release puts the job back with one more attempt', () async {
        await driver.push(_payload('a'));
        ReservedJob first = (await driver.pop('default'))!;
        await driver.release(first, const Duration(milliseconds: 200));
        expect(await driver.pop('default'), isNull);
        await _wait(300);
        ReservedJob again = (await driver.pop('default'))!;
        expect(again.payload.id, 'a');
        expect(again.attempts, 2);
      });

      test('the job of a crashed worker comes back after retryAfter', () async {
        await driver.push(_payload('a'));
        ReservedJob first = (await driver.pop('default'))!;
        expect(first.attempts, 1);
        // the worker crashed: no delete, no release
        expect(await driver.pop('default'), isNull);
        await _wait(500);
        ReservedJob again = (await driver.pop('default'))!;
        expect(again.payload.id, 'a');
        expect(again.attempts, 2);
      });

      test('two workers never get the same job', () async {
        for (int i = 0; i < 20; i++) {
          await driver.push(_payload('job$i'));
        }
        List<ReservedJob?> taken = await Future.wait(<Future<ReservedJob?>>[
          for (int i = 0; i < 25; i++) driver.pop('default'),
        ]);
        List<String> ids = <String>[
          for (ReservedJob? job in taken)
            if (job != null) job.payload.id,
        ];
        expect(ids.length, 20);
        expect(ids.toSet().length, 20);
      });

      test('failed jobs: list, retry, forget and flush', () async {
        await driver.push(_payload('a'));
        await driver.push(_payload('b'));
        await driver.push(_payload('c'));
        for (int i = 0; i < 3; i++) {
          ReservedJob job = (await driver.pop('default'))!;
          await driver.fail(job, 'boom ${job.payload.id}\nstack');
        }
        expect(await driver.pop('default'), isNull);
        List<FailedJob> failed = await driver.failed();
        expect(failed.map(_id), unorderedEquals(<String>['a', 'b', 'c']));
        FailedJob a = failed.firstWhere(_isA);
        expect(a.error, 'boom a\nstack');
        expect(a.queue, 'default');
        expect(a.job, 'SendWelcomeEmail');
        expect(JobPayload.decode(a.payload).attempts, 1);

        expect(await driver.retry('a'), isTrue);
        expect(await driver.retry('a'), isFalse);
        ReservedJob again = (await driver.pop('default'))!;
        expect(again.payload.id, 'a');
        expect(again.attempts, 1);

        expect(await driver.forget('b'), isTrue);
        expect(await driver.forget('b'), isFalse);
        expect((await driver.failed()).map(_id), <String>['c']);
        await driver.flush();
        expect(await driver.failed(), isEmpty);
      });
    }, skip: store.skip);
  }
}

String _id(FailedJob job) => job.id;

bool _isA(FailedJob job) => job.id == 'a';
