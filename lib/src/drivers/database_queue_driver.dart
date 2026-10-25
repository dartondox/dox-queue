import 'dart:convert';

import 'package:dox_orm/dox_orm.dart';

import '../queue_driver.dart';

/// Keeps jobs in the `jobs` table and failed jobs in the `failed_jobs`
/// table (see `QueueTables`), through the dox_orm connection.
///
/// A worker takes a job like this:
/// - Postgres, MySQL 8+ and MariaDB 10.6+: `SELECT ... FOR UPDATE SKIP
///   LOCKED` and an update, in one transaction. Workers do not wait on
///   each other.
/// - SQLite: one `UPDATE ... RETURNING` statement. SQLite runs one write
///   at a time, so two workers never get the same job.
///
/// The worker sets `reserved_at`. When it crashes, the job is free again
/// after [retryAfter]. Times are unix time in milliseconds.
class DatabaseQueueDriver extends QueueDriver {
  DatabaseQueueDriver({
    this.connection,
    this.retryAfter = const Duration(seconds: 90),
  });

  /// the dox_orm connection name. Null: the default connection.
  final String? connection;
  final Duration retryAfter;

  static const String jobsTable = 'jobs';
  static const String failedTable = 'failed_jobs';

  /// a job is free when it is due and not reserved, or when its
  /// reservation is older than [retryAfter]
  static const String _free =
      'queue = @queue AND ((reserved_at IS NULL AND available_at <= @now) '
      'OR reserved_at <= @expired)';

  DBDriver get _db => SqlQueryBuilder().getDBDriver(connection);

  @override
  Future<void> push(
    JobPayload payload, {
    Duration delay = Duration.zero,
  }) async {
    int now = QueueDriver.now();
    await _db.execute(
      'INSERT INTO $jobsTable (queue, payload, attempts, reserved_at, '
      'available_at, created_at) VALUES (@queue, @payload, 0, NULL, '
      '@available, @now)',
      substitutionValues: <String, dynamic>{
        'queue': payload.queue,
        'payload': payload.encode(),
        'available': now + delay.inMilliseconds,
        'now': now,
      },
    );
  }

  @override
  Future<ReservedJob?> pop(String queue) async {
    DBDriver db = _db;
    int now = QueueDriver.now();
    Map<String, dynamic> values = <String, dynamic>{
      'queue': queue,
      'now': now,
      'expired': now - retryAfter.inMilliseconds,
    };
    if (db.getName() == Driver.sqlite) {
      List<Map<String, dynamic>> rows = await db.query(
        'UPDATE $jobsTable SET reserved_at = @now, attempts = attempts + 1 '
        'WHERE id = (SELECT id FROM $jobsTable WHERE $_free ORDER BY id '
        'LIMIT 1) RETURNING id, payload, attempts',
        substitutionValues: values,
      );
      return rows.isEmpty ? null : _reserved(rows.first);
    }
    return db.transaction(() {
      return _popLocked(db, values);
    });
  }

  Future<ReservedJob?> _popLocked(
    DBDriver db,
    Map<String, dynamic> values,
  ) async {
    List<Map<String, dynamic>> rows = await db.query(
      'SELECT id, payload, attempts FROM $jobsTable WHERE $_free '
      'ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED',
      substitutionValues: values,
    );
    if (rows.isEmpty) return null;
    Map<String, dynamic> row = rows.first;
    await db.execute(
      'UPDATE $jobsTable SET reserved_at = @now, attempts = attempts + 1 '
      'WHERE id = @id',
      substitutionValues: <String, dynamic>{
        'now': values['now'],
        'id': _int(row['id']),
      },
    );
    row = <String, dynamic>{...row, 'attempts': _int(row['attempts']) + 1};
    return _reserved(row);
  }

  @override
  Future<void> delete(ReservedJob job) async {
    await _db.execute(
      'DELETE FROM $jobsTable WHERE id = @id',
      substitutionValues: <String, dynamic>{'id': job.ref},
    );
  }

  @override
  Future<void> release(ReservedJob job, Duration delay) async {
    await _db.execute(
      'UPDATE $jobsTable SET reserved_at = NULL, available_at = @at '
      'WHERE id = @id',
      substitutionValues: <String, dynamic>{
        'at': QueueDriver.now() + delay.inMilliseconds,
        'id': job.ref,
      },
    );
  }

  @override
  Future<void> fail(ReservedJob job, String error) async {
    DBDriver db = _db;
    JobPayload payload = job.payload.withAttempts(job.attempts);
    await db.transaction(() async {
      await db.execute(
        'INSERT INTO $failedTable (uuid, queue, payload, exception, '
        'failed_at) VALUES (@uuid, @queue, @payload, @error, @now)',
        substitutionValues: <String, dynamic>{
          'uuid': payload.id,
          'queue': payload.queue,
          'payload': payload.encode(),
          'error': error,
          'now': QueueDriver.now(),
        },
      );
      await db.execute(
        'DELETE FROM $jobsTable WHERE id = @id',
        substitutionValues: <String, dynamic>{'id': job.ref},
      );
    });
  }

  @override
  Future<List<FailedJob>> failed() async {
    List<Map<String, dynamic>> rows = await _db.query(
      'SELECT uuid, queue, payload, exception, failed_at FROM $failedTable '
      'ORDER BY id',
    );
    return <FailedJob>[for (Map<String, dynamic> row in rows) _failed(row)];
  }

  @override
  Future<bool> retry(String id) async {
    DBDriver db = _db;
    return db.transaction(() async {
      FailedJob? job = await _find(db, id);
      if (job == null) return false;
      await push(JobPayload.decode(job.payload).withAttempts(0));
      await _forget(db, id);
      return true;
    });
  }

  @override
  Future<bool> forget(String id) async {
    DBDriver db = _db;
    if (await _find(db, id) == null) return false;
    await _forget(db, id);
    return true;
  }

  @override
  Future<void> flush() async {
    await _db.execute('DELETE FROM $failedTable');
  }

  Future<FailedJob?> _find(DBDriver db, String id) async {
    List<Map<String, dynamic>> rows = await db.query(
      'SELECT uuid, queue, payload, exception, failed_at FROM $failedTable '
      'WHERE uuid = @uuid',
      substitutionValues: <String, dynamic>{'uuid': id},
    );
    return rows.isEmpty ? null : _failed(rows.first);
  }

  Future<void> _forget(DBDriver db, String id) async {
    await db.execute(
      'DELETE FROM $failedTable WHERE uuid = @uuid',
      substitutionValues: <String, dynamic>{'uuid': id},
    );
  }

  static ReservedJob _reserved(Map<String, dynamic> row) {
    return ReservedJob(
      JobPayload.decode(_text(row['payload'])),
      attempts: _int(row['attempts']),
      ref: _int(row['id']),
    );
  }

  static FailedJob _failed(Map<String, dynamic> row) {
    return FailedJob(
      id: _text(row['uuid']),
      queue: _text(row['queue']),
      payload: _text(row['payload']),
      error: _text(row['exception']),
      failedAt: DateTime.fromMillisecondsSinceEpoch(_int(row['failed_at'])),
    );
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
