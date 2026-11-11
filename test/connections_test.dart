import 'package:dox_core/dox_core.dart' show AppConfig, DoxProvider;
import 'package:dox_orm/dox_orm.dart';
import 'package:dox_queue/dox_queue.dart';
import 'package:test/test.dart';

import 'fixtures/jobs.g.dart';
import 'fixtures/test_jobs.dart';
import 'support.dart';

/// the main connection is `sync`, `db` is the sqlite `jobs` table
QueueConfig config = QueueConfig(
  jobs: jobRegistry,
  connections: <String, QueueConfig>{
    'db': QueueConfig(driver: 'database', connection: 'sqlite'),
  },
);

/// the rows of `failed_jobs`, with the `connection` column
Future<List<Map<String, dynamic>>> failedRows() {
  return SqlQueryBuilder()
      .getDBDriver('sqlite')
      .query('SELECT uuid, connection FROM failed_jobs ORDER BY id');
}

/// fail the next job of [driver]
Future<void> failNext(QueueDriver driver) async {
  ReservedJob job = (await driver.pop('default'))!;
  await driver.fail(job, 'boom');
}

void main() {
  setUpAll(setUpTestApp);

  setUp(() async {
    JobLog.clear();
    await testStores.first.reset();
    Queue.configure(config);
  });

  tearDown(Queue.close);

  group('connections', () {
    test('no connection: the main one (here sync, runs at once)', () async {
      await SendWelcomeEmail(userId: 1).dispatch();
      expect(JobLog.handled, <String>['welcome 1']);
      // sync has no job store
      expect(() => Queue.driverOf(null), throwsArgumentError);
    });

    test('dispatch(connection:) puts the job on that connection', () async {
      String id = await SendWelcomeEmail(userId: 2).dispatch(connection: 'db');
      expect(JobLog.handled, isEmpty);
      QueueDriver driver = Queue.driverOf('db');
      expect(driver, isA<DatabaseQueueDriver>());
      expect(Queue.driverOf('db'), same(driver));
      await Worker(
        driver: driver,
        jobs: jobRegistry,
        options: const WorkerOptions(stopWhenEmpty: true),
      ).run();
      expect(JobLog.handled, <String>['welcome 2']);
      expect(id, hasLength(32));
    });

    test('dispatchIf and dispatchUnless take a connection', () async {
      await SendWelcomeEmail(userId: 3).dispatchIf(true, connection: 'db');
      await SendWelcomeEmail(userId: 4).dispatchUnless(false, connection: 'db');
      expect(JobLog.handled, isEmpty);
      expect(await Queue.driverOf('db').pop('default'), isNotNull);
      expect(await Queue.driverOf('db').pop('default'), isNotNull);
    });

    test('an unknown connection throws', () {
      expect(
        SendWelcomeEmail(userId: 1).dispatch(connection: 'nope'),
        throwsArgumentError,
      );
      expect(() => Queue.driverOf('nope'), throwsArgumentError);
    });

    test('a single-driver config works as before', () async {
      Queue.configure(
        QueueConfig(
          driver: 'database',
          jobs: jobRegistry,
          connection: 'sqlite',
        ),
      );
      await SendWelcomeEmail(userId: 5).dispatch();
      expect(Queue.driverOf(null), same(Queue.driver));
      expect(await Queue.driver.pop('default'), isNotNull);
    });

    test('the fake keeps the connection', () async {
      QueueFake fake = Queue.fake();
      try {
        await SendWelcomeEmail(userId: 1).dispatch(connection: 'db');
        await SendWelcomeEmail(userId: 2).dispatch();
        expect(fake.pushed.first.connection, 'db');
        expect(fake.pushed.last.connection, isNull);
      } finally {
        Queue.restore();
      }
    });
  });

  group('queue command --connection', () {
    Future<int> run(List<String> args) {
      return QueueWorker(
        app: AppConfig(
          appKey: 'k' * 32,
          totalIsolate: 1,
          providers: <DoxProvider>[QueueProvider(config)],
        ),
      ).run(args);
    }

    test('parse reads it, null when not given', () {
      expect(QueueCommand.parse(<String>['work']).connection, isNull);
      expect(
        QueueCommand.parse(<String>['work', '--connection=db']).connection,
        'db',
      );
      expect(
        () => QueueCommand.parse(<String>['--connection=']),
        throwsFormatException,
      );
    });

    test('work --connection runs the jobs of that connection', () async {
      await SendWelcomeEmail(userId: 6).dispatch(connection: 'db');
      expect(await run(<String>['work', '--stop-when-empty']), 0);
      expect(JobLog.handled, isEmpty, reason: 'the main one is sync');
      Queue.configure(config);
      expect(
        await run(<String>['work', '--connection=db', '--stop-when-empty']),
        0,
      );
      expect(JobLog.handled, <String>['welcome 6']);
    });

    test('failed --connection reads that connection', () async {
      expect(await run(<String>['failed', '--connection=db']), 0);
      expect(await run(<String>['failed']), 1, reason: 'sync has no store');
      expect(await run(<String>['failed', '--connection=nope']), 1);
    });
  });

  group('failed_jobs.connection', () {
    test('QueueTables makes the column', () async {
      Schema schema = Schema(SqlQueryBuilder().getDBDriver('sqlite'));
      expect(await schema.hasColumn('failed_jobs', 'connection'), isTrue);
    });

    test('a named connection saves its name, the main one NULL', () async {
      await SendWelcomeEmail(userId: 1).dispatch(connection: 'db');
      await failNext(Queue.driverOf('db'));
      QueueDriver main = DatabaseQueueDriver(connection: 'sqlite');
      await SendWelcomeEmail(userId: 2).dispatch(connection: 'db');
      await failNext(main);
      List<Map<String, dynamic>> rows = await failedRows();
      expect(
        rows.map((Map<String, dynamic> row) => row['connection']),
        <Object?>['db', null],
      );
    });

    test('a table without the column keeps working', () async {
      DBDriver db = SqlQueryBuilder().getDBDriver('sqlite');
      await db.execute('DROP TABLE failed_jobs');
      await Schema(db).create('failed_jobs', (Table table) {
        table.id();
        table.string('uuid').unique();
        table.string('queue');
        table.longText('payload');
        table.longText('exception');
        table.bigInteger('failed_at');
      });
      try {
        QueueDriver driver = Queue.driverOf('db');
        String id = await SendWelcomeEmail(userId: 1)
            .dispatch(connection: 'db');
        await failNext(driver);
        await SendWelcomeEmail(userId: 2).dispatch(connection: 'db');
        await failNext(driver);
        List<FailedJob> failed = await driver.failed();
        expect(failed.map((FailedJob job) => job.id), contains(id));
        expect(await driver.retry(id), isTrue);
        expect(await driver.pop('default'), isNotNull);
        expect(await driver.forget(failed.last.id), isTrue);
        await SendWelcomeEmail(userId: 3).dispatch(connection: 'db');
        await failNext(driver);
        expect(
          await driver.pruneFailed(DateTime.now().add(Duration(hours: 1))),
          1,
        );
        await SendWelcomeEmail(userId: 4).dispatch(connection: 'db');
        await failNext(driver);
        await driver.flush();
        expect(await driver.failed(), isEmpty);
      } finally {
        // the next reset makes the tables again
        await QueueTables.down(Schema(db));
        await QueueTables.up(Schema(db));
      }
    });
  });
}
