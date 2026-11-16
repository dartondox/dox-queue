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
    void Function(Table) jobs = switch (schema.driver.getName()) {
      Driver.postgres => _jobsPostgres,
      Driver.sqlite => _jobsSqlite,
      Driver.mysql => _jobsMysql,
    };
    await schema.create(DatabaseQueueDriver.jobsTable, jobs);
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

  /// The columns of the pop index, on Postgres only. A worker finds the
  /// next free job with it when most jobs wait (delayed or reserved).
  /// Postgres picks it only when its statistics say so.
  static const List<String> popIndexColumns = <String>[
    'queue',
    'reserved_at',
    'available_at',
  ];

  /// The columns of the order index, on Postgres and SQLite. It keeps the
  /// jobs of a queue in id order, so a pop stops at the first free job
  /// and sorts nothing. Without it, Postgres with old statistics (a new
  /// table, or one that is often near empty) uses the pop index after a
  /// burst of jobs: each pop then reads and sorts every free job, until
  /// the next ANALYZE. SQLite never uses the pop index well, so it has
  /// only this one. `reserved_at` and `available_at` let SQLite check a
  /// row in the index, without reading the table.
  static const List<String> orderIndexColumns = <String>[
    'queue',
    'id',
    'reserved_at',
    'available_at',
  ];

  /// Add the indexes a worker needs to the `jobs` table: on Postgres the
  /// indexes of [popIndexColumns] and [orderIndexColumns], on SQLite the
  /// index of [orderIndexColumns]. New tables of [up] have them. A table
  /// made before works too, but a poll can read every waiting job. Add
  /// them in a new migration:
  /// ```
  /// await QueueTables.popIndex(schema);
  /// ```
  /// On MySQL and MariaDB it does nothing: the index of `queue` keeps the
  /// rows in id order already. A pop index would make the pop sort the
  /// rows it finds, and `FOR UPDATE` would then lock all of them, so the
  /// other workers would skip every free job.
  ///
  /// It adds only the indexes the table does not have, so it is safe to
  /// call on a table made by [up], or more than once.
  static Future<void> popIndex(Schema schema) async {
    String table = DatabaseQueueDriver.jobsTable;
    List<List<String>> missing = <List<String>>[];
    for (List<String> columns in _indexes(schema.driver.getName())) {
      if (!await schema.hasIndex(table, columns)) missing.add(columns);
    }
    if (missing.isEmpty) return;
    await schema.table(table, (Table jobs) {
      for (List<String> columns in missing) {
        jobs.index(columns);
      }
    });
  }

  /// the indexes of a new `jobs` table on [driver], besides the primary key
  static List<List<String>> _indexes(Driver driver) {
    return switch (driver) {
      Driver.postgres => <List<String>>[popIndexColumns, orderIndexColumns],
      Driver.sqlite => <List<String>>[orderIndexColumns],
      Driver.mysql => <List<String>>[],
    };
  }

  /// Times are unix time in milliseconds. Both indexes start with
  /// `queue`, so they also serve a search by queue alone.
  static void _jobsPostgres(Table table) {
    _jobColumns(table);
    table.index(popIndexColumns);
    table.index(orderIndexColumns);
  }

  /// SQLite: the order index only, see [orderIndexColumns]
  static void _jobsSqlite(Table table) {
    _jobColumns(table);
    table.index(orderIndexColumns);
  }

  /// MySQL and MariaDB: an index of `queue` only, see [popIndex]
  static void _jobsMysql(Table table) {
    _jobColumns(table);
    table.index('queue');
  }

  static void _jobColumns(Table table) {
    table.id();
    table.string('queue');
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
