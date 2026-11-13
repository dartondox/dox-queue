import 'dart:convert';

import 'package:dox_orm/dox_orm.dart';
import 'package:dox_queue/dox_queue.dart';

/// Keeps the batches of `Queue.batch()` in the `job_batches` table (see
/// `QueueTables.upBatches`), in the database connection of
/// `QueueConfig.connection`. Times are unix time in milliseconds.
class DatabaseBatchRepository implements BatchRepository {
  const DatabaseBatchRepository();

  /// the table of the batches
  static const String table = 'job_batches';

  /// the database connection of `QueueConfig.connection`
  DBDriver get _db {
    return SqlQueryBuilder().getDBDriver(Queue.config.connection);
  }

  @override
  Future<void> create(Batch batch) async {
    try {
      await _db.execute(
        'INSERT INTO $table (id, name, total_jobs, pending_jobs, '
        'failed_jobs, options, created_at) VALUES (@id, @name, @total, '
        '@pending, @failed, @options, @now)',
        substitutionValues: <String, dynamic>{
          'id': batch.id,
          'name': batch.name,
          'total': batch.totalJobs,
          'pending': batch.pendingJobs,
          'failed': batch.failedJobs,
          'options': jsonEncode(batch.options),
          'now': batch.createdAt.millisecondsSinceEpoch,
        },
      );
    } catch (error) {
      throw StateError(
        'Can not save the batch in the $table table. Make the table with '
        'QueueTables.upBatches() in a migration (see the '
        'dox_queue_database README), in the database connection of '
        'QueueConfig.connection. The error: $error',
      );
    }
  }

  @override
  Future<Batch?> find(String id) async {
    List<Map<String, dynamic>> rows = await _db.query(
      'SELECT * FROM $table WHERE id = @id',
      substitutionValues: <String, dynamic>{'id': id},
    );
    return rows.isEmpty ? null : _fromRow(rows.first);
  }

  @override
  Future<Batch?> change(
    String id, {
    int total = 0,
    int pending = 0,
    int failed = 0,
    bool onlyOpen = false,
  }) async {
    DBDriver db = _db;
    Map<String, dynamic> values = <String, dynamic>{
      'id': id,
      'total': total,
      'pending': pending,
      'failed': failed,
    };
    String open = onlyOpen
        ? ' AND finished_at IS NULL AND cancelled_at IS NULL'
        : '';
    String update =
        'UPDATE $table SET total_jobs = total_jobs + @total, '
        'pending_jobs = pending_jobs + @pending, '
        // never below 0, also after a count step that threw before
        'failed_jobs = CASE WHEN failed_jobs + @failed < 0 THEN 0 '
        'ELSE failed_jobs + @failed END WHERE id = @id$open';
    if (db.getName() != Driver.mysql) {
      // one statement: SQLite runs one write at a time, Postgres locks
      // the row
      List<Map<String, dynamic>> rows = await db.query(
        '$update RETURNING *',
        substitutionValues: values,
      );
      return rows.isEmpty ? null : _fromRow(rows.first);
    }
    // MySQL and MariaDB have no UPDATE ... RETURNING. The update locks
    // the row until the commit, so the read sees only this change.
    List<Map<String, dynamic>> rows = await db.transaction(() async {
      await db.execute(update, substitutionValues: values);
      return db.query(
        'SELECT * FROM $table WHERE id = @id',
        substitutionValues: values,
      );
    });
    if (rows.isEmpty) return null;
    Batch batch = _fromRow(rows.first);
    // a closed batch: the update did not match
    if (onlyOpen && (batch.finished || batch.cancelled)) return null;
    return batch;
  }

  @override
  Future<void> finish(String id, DateTime at) async {
    await _db.execute(
      'UPDATE $table SET finished_at = @now WHERE id = @id AND '
      'finished_at IS NULL',
      substitutionValues: <String, dynamic>{
        'id': id,
        'now': at.millisecondsSinceEpoch,
      },
    );
  }

  @override
  Future<Batch?> cancel(String id, DateTime at) async {
    DBDriver db = _db;
    Map<String, dynamic> values = <String, dynamic>{
      'id': id,
      'now': at.millisecondsSinceEpoch,
    };
    String update =
        'UPDATE $table SET cancelled_at = @now, finished_at = '
        'COALESCE(finished_at, @now) WHERE id = @id AND cancelled_at IS NULL';
    if (db.getName() != Driver.mysql) {
      List<Map<String, dynamic>> rows = await db.query(
        '$update RETURNING *',
        substitutionValues: values,
      );
      return rows.isEmpty ? null : _fromRow(rows.first);
    }
    // MySQL and MariaDB: lock the row, so only one call cancels it
    return db.transaction(() async {
      String select = 'SELECT * FROM $table WHERE id = @id';
      List<Map<String, dynamic>> rows = await db.query(
        '$select FOR UPDATE',
        substitutionValues: values,
      );
      if (rows.isEmpty || rows.first['cancelled_at'] != null) return null;
      await db.execute(update, substitutionValues: values);
      rows = await db.query(select, substitutionValues: values);
      return _fromRow(rows.first);
    });
  }

  @override
  Future<int> prune(DateTime before, {DateTime? unfinishedBefore}) {
    DBDriver db = _db;
    Map<String, dynamic> values = <String, dynamic>{
      'before': before.millisecondsSinceEpoch,
      'unfinished': ?unfinishedBefore?.millisecondsSinceEpoch,
    };
    String where = unfinishedBefore == null
        ? 'finished_at < @before'
        : '(finished_at < @before OR (finished_at IS NULL AND '
              'created_at < @unfinished))';
    return db.transaction(() async {
      List<Map<String, dynamic>> rows = await db.query(
        'SELECT COUNT(*) AS total FROM $table WHERE $where',
        substitutionValues: values,
      );
      await db.execute(
        'DELETE FROM $table WHERE $where',
        substitutionValues: values,
      );
      return _int(rows.first['total']);
    });
  }

  static Batch _fromRow(Map<String, dynamic> row) {
    return Batch(
      id: _text(row['id']),
      name: _text(row['name']),
      totalJobs: _int(row['total_jobs']),
      pendingJobs: _int(row['pending_jobs']),
      failedJobs: _int(row['failed_jobs']),
      createdAt: _time(row['created_at'])!,
      cancelledAt: _time(row['cancelled_at']),
      finishedAt: _time(row['finished_at']),
      options: jsonDecode(_text(row['options'])) as Map<String, Object?>,
    );
  }

  /// unix time in milliseconds
  static DateTime? _time(Object? value) {
    return value == null
        ? null
        : DateTime.fromMillisecondsSinceEpoch(_int(value));
  }

  /// drivers give a number as int, or as text (eg. mysql bigint)
  static int _int(Object? value) {
    return value is int ? value : int.parse('$value');
  }

  /// MariaDB gives JSON looking text of a long text column as a Map
  static String _text(Object? value) {
    if (value is String) return value;
    if (value is Map || value is List) return jsonEncode(value);
    return '$value';
  }
}
