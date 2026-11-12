import 'dart:io';

import 'package:dox_core/dox_core.dart' hide Schema;
import 'package:dox_orm/dox_orm.dart';
import 'package:dox_queue/dox_queue.dart';

import 'jobs.g.dart';

/// a small app for test/signal_test.dart:
/// `dart run test/fixtures/worker_main.dart <sqlite file> <queue args>`.
/// With the env value `DOWN_REDIS_PORT` it uses the Redis driver on that
/// port instead (nothing listens there) and no database.
Future<void> main(List<String> args) async {
  String? redisPort = Platform.environment['DOWN_REDIS_PORT'];
  AppConfig app = AppConfig(
    appKey: 'k' * 32,
    totalIsolate: 1,
    logger: const LoggerConfig(enabled: true),
    providers: redisPort == null
        ? <DoxProvider>[
            FileDatabaseProvider(args.first),
            QueueProvider(QueueConfig(driver: 'database', jobs: jobRegistry)),
          ]
        : <DoxProvider>[
            QueueProvider(
              QueueConfig(
                driver: 'redis',
                jobs: jobRegistry,
                redis: RedisOptions(port: int.parse(redisPort)),
              ),
            ),
          ],
  );
  exitCode = await QueueWorker(app: app).run(args.skip(1).toList());
}

class FileDatabaseProvider extends DoxProvider {
  FileDatabaseProvider(this.path);

  final String path;

  @override
  void setup() {
    SqlQueryBuilder.initializeWithDatabaseConfig(
      DatabaseConfig(
        connection: 'sqlite',
        connections: <String, ConnectionConfig>{
          'sqlite': ConnectionConfig(
            driver: Driver.sqlite,
            database: path,
            debug: false,
          ),
        },
      ),
    );
  }
}
