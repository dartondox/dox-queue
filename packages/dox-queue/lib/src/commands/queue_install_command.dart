import 'dart:io';

import 'package:dox_annotation/command.dart';
import 'package:dox_annotation/install.dart';

/// `dox queue:install [--driver=database|redis|sync]`. `dox add queue`
/// runs it. It sets up the queue in an app: the config, the provider,
/// `bin/queue.dart`, a sample job and the `REDIS_*` keys of the redis
/// driver. The `database` driver (the default) also adds
/// dox_queue_database and the migration of the jobs tables.
/// It never overwrites a file, so it is safe to run twice.
class QueueInstallCommand extends Command {
  /// [root] is the app folder (the current folder by default). [runner]
  /// runs `dart`, tests pass a fake one.
  QueueInstallCommand({this.root, this.runner});

  final Directory? root;
  final ProcessRunner? runner;

  static const List<String> drivers = <String>['database', 'redis', 'sync'];

  @override
  String get name => 'queue:install';

  @override
  String get description => 'Set up the queue and its worker in this app';

  @override
  String get usage => 'queue:install [--driver=database|redis|sync]';

  @override
  Future<void> run(List<String> args) async {
    String? driver = _driver(args);
    if (driver == null) {
      print(
        'Unknown option. Use --driver=database, --driver=redis or '
        '--driver=sync (database when not given).',
      );
      exitCode = 1;
      return;
    }
    Installer installer = Installer(root: root, runner: runner);
    String app = installer.appName;
    driver = _keptDriver(installer, driver);

    await installer.addDependency('dox_core');
    await installer.addDependency('dox_builder', dev: true);
    await installer.addDependency('build_runner', dev: true);
    if (driver == 'database') {
      await installer.addDependency('dox_queue_database');
    }

    installer.writeFile('lib/config/queue.dart', queueConfig(app, driver));
    installer.writeFile(
      'lib/providers/queue_provider.dart',
      queueProvider(app, driver),
    );
    installer.writeFile('bin/queue.dart', queueEntry(app));
    installer.writeFile('lib/app/jobs/send_welcome_email.job.dart', sampleJob);

    if (driver == 'redis') {
      installer.addEnv('REDIS_HOST', '127.0.0.1');
      installer.addEnv('REDIS_PORT', '6379');
    }

    bool needsMigration = driver == 'database' && _addMigration(installer);
    await installer.buildRunner();
    installer.printSummary(<String>[
      if (driver == 'database' && !needsMigration)
        'The database driver needs the jobs tables. Run `dox add orm` '
            'first, then run `dox add queue` again to get the migration.',
      if (needsMigration) 'Make the jobs tables: `dox migrate`.',
      if (driver == 'redis')
        'Start Redis. Check REDIS_HOST and REDIS_PORT in .env.',
      if (driver != 'sync')
        'Run the worker next to the server: `dox queue:work`.',
      'Send a job: `await SendWelcomeEmail(userId: 1).dispatch();`',
    ]);
  }

  /// the value of `--driver=x` or `--driver x`, database when not given.
  /// Null for an unknown option, a missing value or an unknown driver.
  String? _driver(List<String> args) {
    String driver = 'database';
    for (int i = 0; i < args.length; i++) {
      if (args[i].startsWith('--driver=')) {
        driver = args[i].substring('--driver='.length);
      } else if (args[i] == '--driver' && i + 1 < args.length) {
        driver = args[++i];
      } else {
        return null;
      }
    }
    return drivers.contains(driver) ? driver : null;
  }

  /// the driver of a `lib/config/queue.dart` that is already there, so a
  /// second run with another `--driver` does not add the files and keys of
  /// a driver that the config does not use. [driver] when there is no
  /// config, or no driver in it.
  String _keptDriver(Installer installer, String driver) {
    File config = File('${installer.root.path}/lib/config/queue.dart');
    if (!config.existsSync()) return driver;
    RegExpMatch? kept = RegExp(r'''driver:\s*['"](\w+)['"]''')
        .firstMatch(config.readAsStringSync());
    if (kept == null || kept.group(1) == driver) return driver;
    installer.note(
      'skipped  driver: lib/config/queue.dart already exists, '
      'set the driver there',
    );
    return drivers.contains(kept.group(1)) ? kept.group(1)! : driver;
  }

  /// writes the migration of the jobs tables when the app has
  /// dox_migration. Returns true when the app has it.
  bool _addMigration(Installer installer) {
    if (!installer.hasDependency('dox_migration')) {
      installer.note(
        'skipped  create_queue_tables migration (no dox_migration)',
      );
      return false;
    }
    String? path = MigrationFile(
      folder: Directory('${installer.root.path}/db/migration'),
    ).create('create_queue_tables', migration);
    installer.note(
      path == null
          ? 'skipped  create_queue_tables migration (already there)'
          : 'created  db/migration/${path.split(Platform.pathSeparator).last}',
    );
    return true;
  }

  /// `lib/config/queue.dart` for [driver]
  static String queueConfig(String app, String driver) {
    if (driver != 'redis') {
      return '''
import 'package:dox_queue/dox_queue.dart';
import 'package:$app/generated/jobs.g.dart';

$_configDoc
QueueConfig queueConfig = QueueConfig(driver: '$driver', jobs: jobRegistry);
''';
    }
    return '''
import 'package:dox_queue/dox_queue.dart';
import 'package:$app/generated/jobs.g.dart';

$_configDoc
QueueConfig queueConfig = QueueConfig(
  driver: 'redis',
  jobs: jobRegistry,

  /// The Redis server, from REDIS_URL or REDIS_HOST, REDIS_PORT,
  /// REDIS_USERNAME, REDIS_PASSWORD and REDIS_TLS in .env.
  redis: QueueConfig.redisFromEnv(),
);
''';
  }

  static const String _configDoc = '''
/// Queue settings
/// -------------------------------
/// `driver`: sync, database or redis. The database driver needs the
/// jobs tables (db/migration/..._create_queue_tables.dart).
/// `jobs`: every @Job class, from lib/generated/jobs.g.dart.
/// `inProcess`: true runs jobs inside the web server. Heavy jobs
/// slow it down, so the default runs `dart run bin/queue.dart work`
/// next to the server.''';

  /// `lib/providers/queue_provider.dart` for [driver]
  static String queueProvider(String app, String driver) {
    bool database = driver == 'database';
    String base = database ? 'DatabaseQueueProvider' : 'QueueProvider';
    String import = database
        ? 'package:dox_queue_database/dox_queue_database.dart'
        : 'package:dox_queue/dox_queue.dart';
    String note = database
        ? 'Sets up the queue, so jobs can be dispatched. `DatabaseQueueProvider`\n'
              '/// also turns on the database driver, batches and afterCommit.'
        : 'Sets up the queue, so jobs can be dispatched.';
    return '''
import '$import';
import 'package:$app/config/queue.dart';

/// Queue provider
/// --------------------------
/// $note
/// If this project does not need a queue, delete this file
/// (Dox finds every DoxProvider class, see lib/generated/container.g.dart).
class AppQueueProvider extends $base {
  AppQueueProvider() : super(queueConfig);
}
''';
  }

  /// `bin/queue.dart`, the same as the app template
  static String queueEntry(String app) {
    return '''
import 'dart:io';

import 'package:dox_queue/dox_queue.dart';
import 'package:$app/config/app.dart';

/// run queued jobs: `dart run bin/queue.dart [work] [flags]`, or `dox queue:work`.
/// - work: run jobs until Ctrl+C or SIGTERM.
/// - `failed`, `retry <id|all>`, `forget <id>`, `flush`: manage failed jobs.
Future<void> main(List<String> args) async {
  exitCode = await QueueWorker(app: appConfig).run(args);
}
''';
  }

  /// the sample job of the app template
  static const String sampleJob = '''
import 'package:dox_core/utils/logger.dart';
import 'package:dox_queue/dox_queue.dart';

/// Sends the welcome email of a new user.
/// Dispatch it: `await SendWelcomeEmail(userId: user.id).dispatch();`
@Job()
class SendWelcomeEmail extends DoxJob {
  SendWelcomeEmail({required this.userId});

  final int userId;

  @override
  int get tries => 3;

  @override
  List<Duration> get backoff => const <Duration>[Duration(seconds: 10)];

  @override
  Future<void> handle() async {
    Logger.info('Send a welcome email to user \$userId');
  }
}
''';

  /// the migration of the jobs tables, the same as `dox queue:table`
  /// (dox_queue_database). It is a copy: dox_queue cannot depend on
  /// dox_queue_database.
  static const String migration = '''
// ignore_for_file: file_names
import 'package:dox_migration/dox_migration.dart';
import 'package:dox_queue_database/dox_queue_database.dart';

/// The jobs, failed_jobs and job_batches tables of dox_queue_database.
class CreateQueueTables extends Migration {
  @override
  Future<void> up(Schema schema) async {
    await QueueTables.up(schema);
    await QueueTables.upBatches(schema);
  }

  @override
  Future<void> down(Schema schema) async {
    await QueueTables.downBatches(schema);
    await QueueTables.down(schema);
  }
}
''';
}
