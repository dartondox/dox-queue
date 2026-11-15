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

      test('popAny takes the first queue first, then the oldest job', () async {
        MultiQueuePop many = driver as MultiQueuePop;
        await driver.push(_payload('d1'));
        await driver.push(_payload('h1', queue: 'high'));
        await driver.push(_payload('d2'));
        await driver.push(_payload('h2', queue: 'high'));
        await driver.push(
          _payload('h0', queue: 'high'),
          delay: const Duration(seconds: 30),
        );
        List<String> order = <String>[];
        for (int i = 0; i < 4; i++) {
          ReservedJob job = (await many.popAny(<String>['high', 'default']))!;
          expect(job.attempts, 1);
          order.add(job.payload.id);
        }
        // the delayed job is not due yet
        expect(order, <String>['h1', 'h2', 'd1', 'd2']);
        expect(await many.popAny(<String>['high', 'default']), isNull);
        expect(await many.popAny(<String>[]), isNull);
      });

      test('popAny gives back a job of a crashed worker', () async {
        MultiQueuePop many = driver as MultiQueuePop;
        await driver.push(_payload('a', queue: 'emails'));
        ReservedJob first = (await many.popAny(<String>['x', 'emails']))!;
        expect(first.attempts, 1);
        expect(await many.popAny(<String>['x', 'emails']), isNull);
        await _wait(500);
        ReservedJob again = (await many.popAny(<String>['x', 'emails']))!;
        expect(again.payload.id, 'a');
        expect(again.attempts, 2);
        await driver.delete(again);
        expect(await driver.pop('emails'), isNull);
      });

      test('two workers never get the same job with popAny', () async {
        MultiQueuePop many = driver as MultiQueuePop;
        for (int i = 0; i < 20; i++) {
          await driver.push(_payload('job$i', queue: i.isEven ? 'a' : 'b'));
        }
        List<ReservedJob?> taken = await Future.wait(<Future<ReservedJob?>>[
          for (int i = 0; i < 25; i++) many.popAny(<String>['a', 'b']),
        ]);
        List<String> ids = <String>[
          for (ReservedJob? job in taken)
            if (job != null) job.payload.id,
        ];
        expect(ids.length, 20);
        expect(ids.toSet().length, 20);
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

      test('clear deletes the jobs of one queue', () async {
        await driver.push(_payload('a'));
        await driver.push(_payload('b'), delay: const Duration(minutes: 1));
        await driver.push(_payload('c'));
        await driver.pop('default');
        await driver.push(_payload('d', queue: 'emails'));
        // one ready, one delayed, one reserved
        expect(await driver.clear('default'), 3);
        expect(await driver.pop('default'), isNull);
        expect(await driver.clear('default'), 0);
        expect((await driver.pop('emails'))!.payload.id, 'd');
      });

      test('pruneFailed deletes the old failed jobs', () async {
        await driver.push(_payload('a'));
        await driver.fail((await driver.pop('default'))!, 'boom');
        DateTime past = DateTime.now().subtract(const Duration(hours: 1));
        expect(await driver.pruneFailed(past), 0);
        expect(await driver.failed(), hasLength(1));
        DateTime later = DateTime.now().add(const Duration(seconds: 1));
        expect(await driver.pruneFailed(later), 1);
        expect(await driver.failed(), isEmpty);
      });

      test('release saves the new exception count', () async {
        await driver.push(_payload('a'));
        ReservedJob job = (await driver.pop('default'))!;
        ReservedJob next = ReservedJob(
          job.payload.withExceptions(2),
          attempts: job.attempts,
          ref: job.ref,
        );
        await driver.release(next, Duration.zero);
        ReservedJob again = (await driver.pop('default'))!;
        expect(again.payload.exceptions, 2);
        expect(again.attempts, 2);
      });

      test(
        'pushAll adds many jobs in order, in more than one insert',
        () async {
          // more than one insert of 400 rows: they run in a transaction
          List<JobPayload> jobs = <JobPayload>[
            for (int i = 0; i < 450; i++)
              _payload('job$i', queue: i == 449 ? 'emails' : 'default'),
          ];
          await (driver as BulkPush).pushAll(jobs);
          await (driver as BulkPush).pushAll(<JobPayload>[]);
          for (int i = 0; i < 449; i++) {
            ReservedJob job = (await driver.pop('default'))!;
            expect(job.payload.id, 'job$i');
            expect(job.attempts, 1);
            // the reservation is short here: do not let it come back
            await driver.delete(job);
          }
          expect(await driver.pop('default'), isNull);
          expect((await driver.pop('emails'))!.payload.id, 'job449');
        },
        skip: store.isRedis ? 'the database driver only' : null,
      );

      test('pop still works when Redis does not know the script', () async {
        RedisQueueDriver redis = driver as RedisQueueDriver;
        await driver.push(_payload('a'));
        await driver.push(_payload('b'));
        expect((await driver.pop('default'))!.payload.id, 'a');
        // a SHA1 that Redis does not have, like after a restart. Nothing
        // on the server changes, so a shared Redis is safe.
        redis.debugPopSha = '0' * 40;
        // EVALSHA gives NOSCRIPT: the driver sends the script again
        expect((await driver.pop('default'))!.payload.id, 'b');
        await driver.push(_payload('c'));
        expect((await driver.pop('default'))!.payload.id, 'c');
      }, skip: store.isRedis ? null : 'the Redis driver only');
    }, skip: store.skip);
  }
}

String _id(FailedJob job) => job.id;

bool _isA(FailedJob job) => job.id == 'a';
