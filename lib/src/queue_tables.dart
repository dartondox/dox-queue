import 'package:dox_orm/dox_orm.dart';

import 'drivers/database_queue_driver.dart';

/// The `jobs` and `failed_jobs` tables of the `database` driver.
/// Add a migration in `db/migration` that calls it:
/// ```
/// class CreateQueueTables extends Migration {
///   @override
///   Future<void> up(Schema schema) async {
///     await QueueTables.up(schema);
///   }
///
///   @override
///   Future<void> down(Schema schema) async {
///     await QueueTables.down(schema);
///   }
/// }
/// ```
class QueueTables {
  static Future<void> up(Schema schema) async {
    await schema.create(DatabaseQueueDriver.jobsTable, _jobs);
    await schema.create(DatabaseQueueDriver.failedTable, _failedJobs);
  }

  static Future<void> down(Schema schema) async {
    await schema.dropIfExists(DatabaseQueueDriver.failedTable);
    await schema.dropIfExists(DatabaseQueueDriver.jobsTable);
  }

  /// times are unix time in milliseconds
  static void _jobs(Table table) {
    table.id();
    table.string('queue').index();
    table.longText('payload');
    table.integer('attempts').defaultTo(0);
    table.bigInteger('reserved_at').nullable();
    table.bigInteger('available_at');
    table.bigInteger('created_at');
  }

  static void _failedJobs(Table table) {
    table.id();
    table.string('uuid').unique();
    table.string('queue');
    table.longText('payload');
    table.longText('exception');
    table.bigInteger('failed_at');
  }
}
