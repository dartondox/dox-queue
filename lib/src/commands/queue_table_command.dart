import 'dart:io';

import 'package:dox_core/command.dart';

/// `dox queue:table`: write a migration that makes the `jobs` and
/// `failed_jobs` tables of the database driver (`QueueTables`).
class QueueTableCommand extends Command {
  /// [migrations] is `MigrationFile()` (`db/migration`) by default
  QueueTableCommand({MigrationFile? migrations})
    : _migrations = migrations ?? MigrationFile();

  final MigrationFile _migrations;

  @override
  String get name => 'queue:table';

  @override
  String get description =>
      'Create a migration for the jobs and failed_jobs tables';

  @override
  Future<void> run(List<String> args) async {
    String? path = _migrations.create('create_queue_tables', source);
    if (path == null) {
      print('A create_queue_tables migration is already in db/migration.');
      exitCode = 1;
      return;
    }
    String file = path.split(Platform.pathSeparator).last;
    print('\x1B[32m$file migration created successfully.\x1B[0m');
    print('Run `dox migrate` to make the tables.');
  }

  /// the migration file
  static const String source = '''
// ignore_for_file: file_names
import 'package:dox_migration/dox_migration.dart';
import 'package:dox_queue/dox_queue.dart';

/// The jobs and failed_jobs tables of dox_queue.
class CreateQueueTables extends Migration {
  @override
  Future<void> up(Schema schema) async {
    await QueueTables.up(schema);
  }

  @override
  Future<void> down(Schema schema) async {
    await QueueTables.down(schema);
  }
}
''';
}
