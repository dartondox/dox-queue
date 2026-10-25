import 'dart:io';

import 'package:dox_core/dox_core.dart' hide Schema;
import 'package:dox_orm/dox_orm.dart';
import 'package:dox_queue/dox_queue.dart';

import 'jobs.g.dart';

/// a small app for test/signal_test.dart:
/// `dart run test/fixtures/worker_main.dart <sqlite file> <queue args>`
Future<void> main(List<String> args) async {
  AppConfig app = AppConfig(
    appKey: 'k' * 32,
    totalIsolate: 1,
    logger: const LoggerConfig(enabled: true),
    services: <DoxService>[
      FileDatabaseService(args.first),
      QueueService(QueueConfig(driver: 'database', jobs: jobRegistry)),
    ],
  );
  exitCode = await QueueWorker(app: app).run(args.skip(1).toList());
}

class FileDatabaseService implements DoxService {
  FileDatabaseService(this.path);

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
