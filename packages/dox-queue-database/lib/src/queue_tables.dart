import 'package:dox_orm/dox_orm.dart';

import 'database_batch_repository.dart';
import 'database_queue_driver.dart';

/// The `jobs` and `failed_jobs` tables of the `database` driver.
/// `dox queue:table` writes a migration in `db/migration` that calls it:
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
///
/// Opt-in: `QueueTables.up(schema, failedAtTimestamp: true)` makes
/// `failed_jobs.failed_at` a timestamp column (UTC), not unix time in
/// milliseconds. Then set `QueueConfig(failedAtTimestamp: true)` too.
/// The queue checks once that the setting matches the column, and throws
/// a [StateError] when it does not.
///
/// Opt-in: `QueueTables.upBatches(schema)` makes the `job_batches` table
/// of `Queue.batch()`. It is a separate call, so an app that ran the
/// first migration adds it in a new one. Only apps with batches need it,
/// also with the Redis driver.
class QueueTables {
  static Future<void> up(
    Schema schema, {
    bool failedAtTimestamp = false,
  }) async {
    await schema.create(DatabaseQueueDriver.jobsTable, _jobs);
    // NEXT MAJOR: make this the default, then remove the flag.
    await schema.create(
      DatabaseQueueDriver.failedTable,
      failedAtTimestamp ? _failedJobsWithTimestamp : _failedJobs,
    );
  }

  static Future<void> down(Schema schema) async {
    await schema.dropIfExists(DatabaseQueueDriver.failedTable);
    await schema.dropIfExists(DatabaseQueueDriver.jobsTable);
  }

  /// The `job_batches` table of `Queue.batch()`, in the database
  /// connection of `QueueConfig.connection`. Times are unix time in
  /// milliseconds.
  static Future<void> upBatches(Schema schema) async {
    await schema.create(DatabaseBatchRepository.table, _batches);
  }

  static Future<void> downBatches(Schema schema) async {
    await schema.dropIfExists(DatabaseBatchRepository.table);
  }

  static void _batches(Table table) {
    table.string('id').primary();
    table.string('name');
    table.integer('total_jobs');
    table.integer('pending_jobs');
    table.integer('failed_jobs');
    table.longText('options');
    table.bigInteger('cancelled_at').nullable();
    table.bigInteger('created_at');
    table.bigInteger('finished_at').nullable().index();
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

  /// `connection` is the name of the queue connection of
  /// `QueueConfig.connections`, NULL for the main connection. A table
  /// without it works too: add it with
  /// `table.string('connection').nullable()` in a new migration.
  static void _failedJobs(Table table) {
    _failedJobColumns(table);
    table.bigInteger('failed_at');
  }

  /// `failed_at` is a timestamp in UTC: TIMESTAMP on SQLite, Postgres
  /// (without time zone) and MySQL
  static void _failedJobsWithTimestamp(Table table) {
    _failedJobColumns(table);
    table.timestamp('failed_at');
  }

  static void _failedJobColumns(Table table) {
    table.id();
    table.string('uuid').unique();
    table.string('connection').nullable();
    table.string('queue');
    table.longText('payload');
    table.longText('exception');
  }
}
