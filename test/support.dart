import 'dart:io';

import 'package:dox_core/dox_core.dart' show AppConfig, Dox;
import 'package:dox_orm/dox_orm.dart';
import 'package:dox_queue/dox_queue.dart';

/// The test stores. SQLite always runs. The others run when their port
/// is set: `PG_PORT`, `MYSQL_PORT` (MariaDB), `REDIS_PORT`.
Map<String, String> env = Platform.environment;

DatabaseConfig testDatabaseConfig = DatabaseConfig(
  connection: 'sqlite',
  connections: <String, ConnectionConfig>{
    'sqlite': ConnectionConfig(
      driver: Driver.sqlite,
      database: ':memory:',
      debug: false,
    ),
    'postgres': ConnectionConfig(
      driver: Driver.postgres,
      host: '127.0.0.1',
      port: int.parse(env['PG_PORT'] ?? '5432'),
      user: 'postgres',
      password: 'postgres',
      database: 'postgres',
      debug: false,
    ),
    'mysql': ConnectionConfig(
      driver: Driver.mysql,
      host: '127.0.0.1',
      port: int.parse(env['MYSQL_PORT'] ?? '3306'),
      user: 'dox',
      password: 'password',
      database: 'dox-framework',
      debug: false,
    ),
  },
);

/// One store to test: a name, and how to make and clean it.
class TestStore {
  TestStore(this.name, {this.skip});

  /// `sqlite`, `postgres`, `mysql` or `redis`
  final String name;

  /// why the store does not run, or null
  final String? skip;

  bool get isRedis => name == 'redis';

  /// a queue config for this store
  QueueConfig config({
    Duration retryAfter = const Duration(seconds: 90),
    JobRegistry jobs = const JobRegistry(<JobCodec>[]),
  }) {
    if (isRedis) {
      return QueueConfig(
        driver: 'redis',
        jobs: jobs,
        redis: RedisOptions(port: int.parse(env['REDIS_PORT']!)),
        prefix: 'dox_queue_test:',
        retryAfter: retryAfter,
      );
    }
    return QueueConfig(
      driver: 'database',
      jobs: jobs,
      connection: name,
      retryAfter: retryAfter,
    );
  }

  /// empty tables or keys. Makes the tables on first use.
  Future<void> reset() async {
    if (isRedis) {
      RedisQueueDriver driver = config().createDriver() as RedisQueueDriver;
      List<Object?> keys = await driver.redis.sendCommand(<String>[
        'KEYS',
        'dox_queue_test:*',
      ]) as List<Object?>;
      if (keys.isNotEmpty) {
        await driver.redis.sendCommand(<String>['DEL', ...keys.cast<String>()]);
      }
      await driver.close();
      return;
    }
    DBDriver db = SqlQueryBuilder().getDBDriver(name);
    if (!_made.contains(name)) {
      Schema schema = Schema(db);
      await QueueTables.down(schema);
      await QueueTables.up(schema);
      _made.add(name);
    }
    await db.execute('DELETE FROM jobs');
    await db.execute('DELETE FROM failed_jobs');
  }

  static final Set<String> _made = <String>{};
}

List<TestStore> testStores = <TestStore>[
  TestStore('sqlite'),
  TestStore('postgres', skip: env['PG_PORT'] == null ? 'no PG_PORT' : null),
  TestStore('mysql', skip: env['MYSQL_PORT'] == null ? 'no MYSQL_PORT' : null),
  TestStore('redis', skip: env['REDIS_PORT'] == null ? 'no REDIS_PORT' : null),
];

/// set up Dox (for the logger) and the ORM once per test file
void setUpTestApp() {
  Dox().initialize(AppConfig(appKey: 'k' * 32, totalIsolate: 1));
  SqlQueryBuilder.initializeWithDatabaseConfig(testDatabaseConfig);
}
