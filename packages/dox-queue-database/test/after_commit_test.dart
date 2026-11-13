import 'package:dox_orm/dox_orm.dart';
import 'package:dox_queue/dox_queue.dart';
import 'package:dox_queue_database/dox_queue_database.dart';
import 'package:test/test.dart';

import 'fixtures/jobs.g.dart';
import 'fixtures/test_jobs.dart';
import 'support.dart';

TestStore _sqlite = testStores.first;

/// a queue on the `jobs` table of the dox_orm connection 'sqlite'.
/// The transactions below run on the default dox_orm connection.
QueueConfig _database({bool afterCommit = false}) {
  return QueueConfig(
    driver: 'database',
    jobs: jobRegistry,
    connection: 'sqlite',
    afterCommit: afterCommit,
  );
}

Future<bool> _queued() async => await Queue.driver.pop('default') != null;

void main() {
  setUpAll(setUpTestApp);

  setUp(() async {
    JobLog.clear();
    await _sqlite.reset();
  });

  tearDown(() async {
    Queue.restore();
    await Queue.close();
  });

  group('database driver', () {
    test('afterCommit pushes when the transaction commits', () async {
      Queue.configure(_database());
      String? inside;
      String id = await QueryBuilder.transaction(() async {
        String id = await SendWelcomeEmail(userId: 1)
            .dispatch(afterCommit: true);
        inside = '${await _queued()}';
        return id;
      });
      expect(inside, 'false');
      expect(id, hasLength(32));
      ReservedJob? reserved = await Queue.driver.pop('default');
      expect(reserved?.payload.id, id);
    });

    test('afterCommit drops the job on rollback', () async {
      Queue.configure(_database());
      await expectLater(
        QueryBuilder.transaction(() async {
          await SendWelcomeEmail(userId: 1).dispatch(afterCommit: true);
          throw StateError('stop');
        }),
        throwsStateError,
      );
      expect(await _queued(), false);
    });

    test('only a transaction of the default connection counts', () async {
      const OrmTransactions transactions = OrmTransactions();
      await QueryBuilder.transaction(() async {
        expect(transactions.inTransaction, isTrue);
      });
      await QueryBuilder.transaction(() async {
        expect(transactions.inTransaction, isFalse);
      }, connection: 'sqlite_other');
    });

    test('a transaction of a named connection pushes at once', () async {
      Queue.configure(_database());
      await QueryBuilder.transaction(() async {
        await SendWelcomeEmail(userId: 1).dispatch(afterCommit: true);
        expect(await _queued(), true);
      }, connection: 'sqlite_other');
    });

    test('afterCommit pushes at once outside a transaction', () async {
      Queue.configure(_database());
      await SendWelcomeEmail(userId: 1).dispatch(afterCommit: true);
      expect(await _queued(), true);
    });

    test('without afterCommit the job is pushed at once', () async {
      Queue.configure(_database());
      await QueryBuilder.transaction(() async {
        await SendWelcomeEmail(userId: 1).dispatch();
        expect(await _queued(), true);
      });
    });

    test('the setting turns it on for every dispatch', () async {
      Queue.configure(_database(afterCommit: true));
      await QueryBuilder.transaction(() async {
        await SendWelcomeEmail(userId: 1).dispatchIf(true);
        expect(await _queued(), false);
      });
      expect(await _queued(), true);
    });

    test('afterCommit: false wins over the setting', () async {
      Queue.configure(_database(afterCommit: true));
      await QueryBuilder.transaction(() async {
        await SendWelcomeEmail(userId: 1)
            .dispatchUnless(false, afterCommit: false);
        expect(await _queued(), true);
      });
    });

    test('a push that fails after the commit throws, the data stays', () async {
      // the dox_orm connection 'missing' does not exist, so the push fails
      Queue.configure(
        QueueConfig(
          driver: 'database',
          jobs: jobRegistry,
          connection: 'missing',
        ),
      );
      await QueryBuilder.query(
        'CREATE TABLE IF NOT EXISTS ac_rows (id INTEGER)',
      );
      await QueryBuilder.query('DELETE FROM ac_rows');
      addTearDown(() => QueryBuilder.query('DROP TABLE IF EXISTS ac_rows'));
      await expectLater(
        QueryBuilder.transaction(() async {
          await QueryBuilder.query('INSERT INTO ac_rows (id) VALUES (1)');
          await SendWelcomeEmail(userId: 1).dispatch(afterCommit: true);
        }),
        throwsA(anything),
      );
      List<Map<String, dynamic>> rows = await QueryBuilder.query(
        'SELECT id FROM ac_rows',
      );
      expect(rows, hasLength(1));
    });

    test('a deferred push goes to the named queue connection', () async {
      Queue.configure(
        QueueConfig(
          jobs: jobRegistry,
          connections: <String, QueueConfig>{'db': _database()},
        ),
      );
      await QueryBuilder.transaction(() async {
        await SendWelcomeEmail(userId: 1)
            .dispatch(connection: 'db', afterCommit: true);
        expect(await Queue.driverOf('db').pop('default'), isNull);
      });
      expect(await Queue.driverOf('db').pop('default'), isNotNull);
      expect(JobLog.handled, isEmpty);
    });

    test('a job that is not in the registry throws at dispatch', () async {
      Queue.configure(QueueConfig(driver: 'database', connection: 'sqlite'));
      await expectLater(
        QueryBuilder.transaction(() async {
          await SendWelcomeEmail(userId: 1).dispatch(afterCommit: true);
        }),
        throwsStateError,
      );
    });
  });

  group('sync driver', () {
    test('afterCommit runs the job after the commit', () async {
      Queue.configure(QueueConfig(jobs: jobRegistry));
      await QueryBuilder.transaction(() async {
        await Queue.dispatch(SendWelcomeEmail(userId: 1), afterCommit: true);
        expect(JobLog.handled, isEmpty);
      });
      expect(JobLog.handled, <String>['welcome 1']);
    });

    test('afterCommit does not run the job on rollback', () async {
      Queue.configure(QueueConfig(jobs: jobRegistry, serializeSync: true));
      await expectLater(
        QueryBuilder.transaction(() async {
          await Queue.dispatch(SendWelcomeEmail(userId: 1), afterCommit: true);
          throw StateError('stop');
        }),
        throwsStateError,
      );
      expect(JobLog.handled, isEmpty);
    });
  });

  test('the fake keeps the job at dispatch time', () async {
    QueueFake fake = Queue.fake();
    await expectLater(
      QueryBuilder.transaction(() async {
        await SendWelcomeEmail(userId: 1).dispatch(afterCommit: true);
        throw StateError('stop');
      }),
      throwsStateError,
    );
    fake.assertPushed<SendWelcomeEmail>();
  });
}
