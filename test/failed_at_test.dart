import 'package:dox_orm/dox_orm.dart';
import 'package:dox_queue/dox_queue.dart';
import 'package:test/test.dart';

import 'support.dart';

/// The steps of the queue docs: a `failed_jobs` table with the integer
/// `failed_at` column gets a timestamp column with the same times.
const Map<String, List<String>> _toTimestamp = <String, List<String>>{
  'sqlite': <String>[
    'ALTER TABLE failed_jobs ADD COLUMN failed_at_utc TIMESTAMP',
    "UPDATE failed_jobs SET failed_at_utc = strftime('%Y-%m-%dT%H:%M:%fZ', "
        "failed_at / 1000.0, 'unixepoch')",
    'ALTER TABLE failed_jobs DROP COLUMN failed_at',
    'ALTER TABLE failed_jobs RENAME COLUMN failed_at_utc TO failed_at',
  ],
  'postgres': <String>[
    'ALTER TABLE failed_jobs ALTER COLUMN failed_at TYPE TIMESTAMP '
        "USING to_timestamp(failed_at / 1000.0) AT TIME ZONE 'UTC'",
  ],
  'mysql': <String>[
    'ALTER TABLE failed_jobs ADD COLUMN failed_at_utc TIMESTAMP NULL',
    "UPDATE failed_jobs SET failed_at_utc = DATE_ADD('1970-01-01 00:00:00', "
        'INTERVAL failed_at DIV 1000 SECOND)',
    'ALTER TABLE failed_jobs DROP COLUMN failed_at',
    'ALTER TABLE failed_jobs CHANGE failed_at_utc failed_at TIMESTAMP NOT NULL',
  ],
};

JobPayload _payload(String id) {
  return JobPayload(
    id: id,
    job: 'SendWelcomeEmail',
    queue: 'default',
    data: '{}',
  );
}

String _id(FailedJob job) => job.id;

void main() {
  setUpAll(setUpTestApp);

  for (TestStore store in testStores.where((TestStore s) => !s.isRedis)) {
    DBDriver db() => SqlQueryBuilder().getDBDriver(store.name);

    /// new tables of one shape
    Future<void> makeTables({required bool timestamp}) async {
      Schema schema = Schema(db());
      await QueueTables.down(schema);
      await QueueTables.up(schema, failedAtTimestamp: timestamp);
    }

    DatabaseQueueDriver driverOf({required bool timestamp}) {
      return QueueConfig(
            driver: 'database',
            connection: store.name,
            failedAtTimestamp: timestamp,
          ).createDriver()
          as DatabaseQueueDriver;
    }

    /// push a job, take it and let it fail
    Future<void> failJob(QueueDriver driver, String id) async {
      await driver.push(_payload(id));
      await driver.fail((await driver.pop('default'))!, 'boom');
    }

    for (bool timestamp in <bool>[false, true]) {
      group('${store.name} failedAtTimestamp: $timestamp', () {
        late QueueDriver driver;

        setUp(() async {
          await makeTables(timestamp: timestamp);
          driver = driverOf(timestamp: timestamp);
        });

        test('the column and the saved value', () async {
          // MySQL keeps whole seconds
          DateTime start = DateTime.now().subtract(const Duration(seconds: 1));
          await failJob(driver, 'a');
          List<Map<String, dynamic>> rows = await db().query(
            'SELECT failed_at FROM failed_jobs',
          );
          Object? value = rows.single['failed_at'];
          expect(value, timestamp ? isA<DateTime>() : isNot(isA<DateTime>()));
          FailedJob job = (await driver.failed()).single;
          expect(job.failedAt.isUtc, isFalse);
          expect(job.failedAt.isAfter(start), isTrue);
          expect(job.failedAt.isAfter(DateTime.now()), isFalse);
        });

        test('list, retry, forget and flush', () async {
          await failJob(driver, 'a');
          await failJob(driver, 'b');
          await failJob(driver, 'c');
          expect((await driver.failed()).map(_id), <String>['a', 'b', 'c']);
          expect(await driver.retry('a'), isTrue);
          expect((await driver.pop('default'))!.payload.id, 'a');
          expect(await driver.forget('b'), isTrue);
          expect(await driver.forget('b'), isFalse);
          expect((await driver.failed()).map(_id), <String>['c']);
          await driver.flush();
          expect(await driver.failed(), isEmpty);
        });

        test('pruneFailed deletes by age', () async {
          DateTime old = DateTime.utc(2020, 1, 2, 3, 4, 5, 6);
          await db().execute(
            'INSERT INTO failed_jobs (uuid, queue, payload, exception, '
            "failed_at) VALUES ('old', 'default', @payload, 'boom', @at)",
            substitutionValues: <String, dynamic>{
              'payload': _payload('old').encode(),
              'at': timestamp ? old : old.millisecondsSinceEpoch,
            },
          );
          await failJob(driver, 'new');
          DateTime day = DateTime.now().subtract(const Duration(days: 1));
          expect(await driver.pruneFailed(day), 1);
          expect((await driver.failed()).map(_id), <String>['new']);
        });

        test('a table of the other shape throws, and moves nothing', () async {
          await makeTables(timestamp: !timestamp);
          await driver.push(_payload('a'));
          ReservedJob job = (await driver.pop('default'))!;
          await expectLater(driver.fail(job, 'boom'), throwsStateError);
          // the check runs once, the error stays
          await expectLater(driver.failed(), throwsStateError);
          await expectLater(driver.retry('a'), throwsStateError);
          await expectLater(driver.forget('a'), throwsStateError);
          await expectLater(
            driver.pruneFailed(DateTime.now()),
            throwsStateError,
          );
          List<Map<String, dynamic>> failed = await db().query(
            'SELECT uuid FROM failed_jobs',
          );
          expect(failed, isEmpty);
          List<Map<String, dynamic>> jobs = await db().query(
            'SELECT id FROM jobs',
          );
          expect(jobs, hasLength(1));
        });
      }, skip: store.skip);
    }

    test('${store.name}: the docs steps keep the failed_at times', () async {
      await makeTables(timestamp: false);
      QueueDriver before = driverOf(timestamp: false);
      await failJob(before, 'a');
      // a time with milliseconds, to see them kept
      int millis = DateTime.utc(
        2024,
        5,
        6,
        7,
        8,
        9,
        999,
      ).millisecondsSinceEpoch;
      await db().execute(
        'INSERT INTO failed_jobs (uuid, queue, payload, exception, '
        "failed_at) VALUES ('b', 'default', @payload, 'boom', @at)",
        substitutionValues: <String, dynamic>{
          'payload': _payload('b').encode(),
          'at': millis,
        },
      );
      List<FailedJob> old = await before.failed();
      await before.close();

      for (String sql in _toTimestamp[store.name]!) {
        await db().execute(sql);
      }

      QueueDriver after = driverOf(timestamp: true);
      List<FailedJob> now = await after.failed();
      expect(now.map(_id), <String>['a', 'b']);
      // MySQL TIMESTAMP keeps whole seconds
      int precision = store.name == 'mysql' ? 1000 : 1;
      for (int i = 0; i < old.length; i++) {
        int was = old[i].failedAt.millisecondsSinceEpoch;
        expect(now[i].failedAt.millisecondsSinceEpoch, was - was % precision);
      }
      await failJob(after, 'c');
      expect(
        await after.pruneFailed(DateTime.utc(2025)),
        1, // b, of 2024
      );
      expect((await after.failed()).map(_id), <String>['a', 'c']);
      await after.close();
    }, skip: store.skip);
  }
}
