import 'package:dox_core/dox_core.dart' show AppConfig, Dox, LoggerConfig;
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
    'UPDATE failed_jobs SET failed_at_utc = FROM_UNIXTIME(failed_at DIV 1000)',
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

/// a driver whose first [failures] column type queries throw, like a
/// connection that dropped
class _FlakyDriver extends DBDriver {
  _FlakyDriver(this.inner);

  final DBDriver inner;
  int failures = 1;

  /// how many column type queries ran, the failed ones too
  int typeQueries = 0;

  @override
  Driver getName() => inner.getName();

  @override
  bool get supportsReturning => inner.supportsReturning;

  @override
  Future<List<Map<String, dynamic>>> query(
    String query, {
    String? primaryKey,
    Map<String, dynamic>? substitutionValues,
  }) async {
    bool typeQuery =
        query.contains('pragma_table_info') ||
        query.contains('information_schema');
    if (typeQuery) typeQueries++;
    if (typeQuery && failures > 0) {
      failures--;
      throw Exception('connection lost');
    }
    return inner.query(
      query,
      primaryKey: primaryKey,
      substitutionValues: substitutionValues,
    );
  }

  @override
  Future<void> execute(
    String query, {
    Map<String, dynamic>? substitutionValues,
  }) {
    return inner.execute(query, substitutionValues: substitutionValues);
  }

  @override
  Future<R> transaction<R>(Future<R> Function() callback) {
    return inner.transaction(callback);
  }
}

void main() {
  setUpAll(setUpTestApp);

  test('each queue connection has its own setting', () async {
    DBDriver mainDb = SqlQueryBuilder().getDBDriver('sqlite');
    DBDriver otherDb = SqlQueryBuilder().getDBDriver('sqlite_other');
    await QueueTables.down(Schema(mainDb));
    await QueueTables.up(Schema(mainDb));
    await QueueTables.down(Schema(otherDb));
    await QueueTables.up(Schema(otherDb), failedAtTimestamp: true);
    QueueConfig config = QueueConfig(
      driver: 'database',
      connection: 'sqlite',
      connections: <String, QueueConfig>{
        'other': QueueConfig(
          driver: 'database',
          connection: 'sqlite_other',
          failedAtTimestamp: true,
        ),
      },
    );
    QueueDriver main = config.createDriver();
    QueueDriver other = config
        .connectionOf('other')
        .createDriver(name: 'other');
    for (QueueDriver driver in <QueueDriver>[main, other]) {
      await driver.push(_payload('a'));
      await driver.fail((await driver.pop('default'))!, 'boom');
      expect((await driver.failed()).map(_id), <String>['a']);
    }
    Future<Object?> value(DBDriver db) async {
      return (await db.query('SELECT failed_at FROM failed_jobs'))
          .single['failed_at'];
    }

    expect(await value(mainDb), isA<int>());
    expect(await value(otherDb), isA<DateTime>());
  });

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
          if (timestamp && store.name == 'mysql') {
            // the true instant, in any session time zone
            List<Map<String, dynamic>> unix = await db().query(
              'SELECT UNIX_TIMESTAMP(failed_at) AS t FROM failed_jobs',
            );
            expect(
              int.parse('${unix.single['t']}'),
              job.failedAt.millisecondsSinceEpoch ~/ 1000,
            );
          }
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

        test('a type query that fails is not kept', () async {
          // log on, so the test can read the warning
          Dox().initialize(
            AppConfig(
              appKey: 'k' * 32,
              totalIsolate: 1,
              logger: LoggerConfig(enabled: true),
            ),
          );
          DBDriver old = SqlQueryBuilder().dbDriver;
          _FlakyDriver flakyDb = _FlakyDriver(db());
          SqlQueryBuilder().dbDriver = flakyDb;
          QueueDriver flaky = QueueConfig(
            driver: 'database',
            failedAtTimestamp: timestamp,
          ).createDriver();
          try {
            await flaky.push(_payload('a'));
            ReservedJob job = (await flaky.pop('default'))!;
            String log = await logOf(() async {
              if (timestamp) {
                // the setting on: no blind write, the error goes up
                await expectLater(flaky.fail(job, 'boom'), throwsException);
                expect(await flaky.failed(), isEmpty);
              } else {
                // the setting off: it goes on as without the check
                await flaky.fail(job, 'boom');
              }
            });
            expect(
              'can not read the type'.allMatches(log),
              hasLength(timestamp ? 0 : 1),
            );
            // the failed read was not kept: the next call reads again
            if (!timestamp) await failJob(flaky, 'b');
            expect(flakyDb.typeQueries, 2);
            // the good result is kept: no more type queries
            if (timestamp) await flaky.fail(job, 'boom');
            await failJob(flaky, 'c');
            expect(flakyDb.typeQueries, 2);
            expect(
              (await flaky.failed()).map(_id),
              timestamp ? <String>['a', 'c'] : <String>['a', 'b', 'c'],
            );
          } finally {
            SqlQueryBuilder().dbDriver = old;
            Dox().initialize(AppConfig(appKey: 'k' * 32, totalIsolate: 1));
          }
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
      // 01:30 of New York, the second time, in the hour that repeats
      int millis = DateTime.utc(
        2024,
        11,
        3,
        6,
        30,
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
      if (store.name == 'mysql') {
        List<Map<String, dynamic>> unix = await db().query(
          'SELECT UNIX_TIMESTAMP(failed_at) AS t FROM failed_jobs '
          "WHERE uuid = 'b'",
        );
        expect(int.parse('${unix.single['t']}'), millis ~/ 1000);
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
