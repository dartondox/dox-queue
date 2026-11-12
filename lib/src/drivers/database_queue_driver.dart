import 'dart:convert';

import 'package:dox_orm/dox_orm.dart';

import '../queue.dart';
import '../queue_log.dart';
import '../queue_driver.dart';
import '../unique_lock.dart';

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
/// after [retryAfter]. Times are unix time in milliseconds, but
/// `failed_jobs.failed_at` is a UTC timestamp when [failedAtTimestamp] is
/// true.
class DatabaseQueueDriver extends QueueDriver {
  DatabaseQueueDriver({
    this.connection,
    this.retryAfter = const Duration(seconds: 90),
    this.queueConnection,
    this.failedAtTimestamp = false,
  });

  /// the dox_orm connection name. Null: the default connection.
  final String? connection;
  final Duration retryAfter;

  /// The name of the queue connection (see `QueueConfig.connections`).
  /// A failed job saves it in `failed_jobs.connection`, when the table
  /// has that column. Null: the main connection, the column stays NULL.
  final String? queueConnection;

  /// true when `failed_jobs` has the `connection` column. Read once, on
  /// the first failed job of a named connection. A table made before
  /// the column existed works without it. After you add the column,
  /// restart the workers.
  bool? _hasConnectionColumn;

  /// Opt-in: `failed_jobs.failed_at` is a timestamp column (UTC), not
  /// unix time in milliseconds. See `QueueConfig.failedAtTimestamp`.
  final bool failedAtTimestamp;

  /// true after the `failed_at` column type matched [failedAtTimestamp]
  bool _failedAtMatches = false;

  /// the error of a `failed_at` column that does not match
  String? _failedAtMismatch;

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
      'UPDATE $jobsTable SET reserved_at = NULL, available_at = @at, '
      'attempts = @attempts, payload = @payload WHERE id = @id',
      substitutionValues: <String, dynamic>{
        'at': QueueDriver.now() + delay.inMilliseconds,
        'attempts': job.attempts,
        'payload': job.payload.encode(),
        'id': job.ref,
      },
    );
  }

  @override
  Future<void> fail(ReservedJob job, String error) async {
    DBDriver db = _db;
    JobPayload payload = job.payload.withAttempts(job.attempts);
    String? name = queueConnection;
    await _checkFailedAt(db);
    String at = _unixTime(db) ? 'FROM_UNIXTIME(@now DIV 1000)' : '@now';
    // outside the transaction: on Postgres a failed query there would
    // break it. The main connection never reads it: NULL is the default.
    bool withName =
        name != null &&
        (_hasConnectionColumn ??= await Schema(db)
            .hasColumn(failedTable, 'connection'));
    await db.transaction(() async {
      await db.execute(
        withName
            ? 'INSERT INTO $failedTable (uuid, connection, queue, payload, '
                  'exception, failed_at) VALUES (@uuid, @connection, @queue, '
                  '@payload, @error, $at)'
            : 'INSERT INTO $failedTable (uuid, queue, payload, exception, '
                  'failed_at) VALUES (@uuid, @queue, @payload, @error, $at)',
        substitutionValues: <String, dynamic>{
          'uuid': payload.id,
          'connection': ?(withName ? name : null),
          'queue': payload.queue,
          'payload': payload.encode(),
          'error': error,
          'now': _failedAtValue(db, QueueDriver.now()),
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
    DBDriver db = _db;
    await _checkFailedAt(db);
    List<Map<String, dynamic>> rows = await db.query(
      'SELECT uuid, queue, payload, exception, ${_failedAtColumn(db)} '
      'FROM $failedTable ORDER BY id',
    );
    return <FailedJob>[for (Map<String, dynamic> row in rows) _failed(row)];
  }

  @override
  Future<bool> retry(String id) async {
    DBDriver db = _db;
    await _checkFailedAt(db);
    return db.transaction(() async {
      FailedJob? job = await _find(db, id);
      if (job == null) return false;
      await push(Queue.retriedPayload(job.payload));
      await _forget(db, id);
      return true;
    });
  }

  @override
  Future<bool> forget(String id) async {
    DBDriver db = _db;
    await _checkFailedAt(db);
    if (await _find(db, id) == null) return false;
    await _forget(db, id);
    return true;
  }

  @override
  Future<void> flush() async {
    await _db.execute('DELETE FROM $failedTable');
  }

  @override
  Future<int> clear(String queue) async {
    DBDriver db = _db;
    List<String> unique = <String>[];
    int total = await db.transaction(() async {
      // the jobs up to the last id now: a job pushed while this runs is
      // kept, so its unique lock is never freed by mistake
      List<Map<String, dynamic>> rows = await db.query(
        'SELECT MAX(id) AS top FROM $jobsTable WHERE queue = @queue',
        substitutionValues: <String, dynamic>{'queue': queue},
      );
      Object? top = rows.first['top'];
      if (top == null) return 0;
      Map<String, dynamic> values = <String, dynamic>{
        'queue': queue,
        'top': _int(top),
      };
      String where = 'queue = @queue AND id <= @top';
      // ponytail: a LIKE scan over the queue, to read only the unique
      // jobs. Fine for a clear, an admin command.
      rows = await db.query(
        'SELECT payload FROM $jobsTable WHERE $where AND payload LIKE '
        "'%\"unique\":%'",
        substitutionValues: values,
      );
      for (Map<String, dynamic> row in rows) {
        unique.add(_text(row['payload']));
      }
      rows = await db.query(
        'SELECT COUNT(*) AS total FROM $jobsTable WHERE $where',
        substitutionValues: values,
      );
      await db.execute(
        'DELETE FROM $jobsTable WHERE $where',
        substitutionValues: values,
      );
      return _int(rows.first['total']);
    });
    // after the commit: a rollback must not free the locks
    for (String text in unique) {
      await freeUniqueLockOf(text);
    }
    return total;
  }

  @override
  Future<int> pruneFailed(DateTime before) async {
    DBDriver db = _db;
    await _checkFailedAt(db);
    return _deleteWhere(
      failedTable,
      _unixTime(db)
          ? 'UNIX_TIMESTAMP(failed_at) * 1000 < @value'
          : 'failed_at < @value',
      _failedAtValue(db, before.millisecondsSinceEpoch),
    );
  }

  /// True when `failed_at` is a MySQL TIMESTAMP. It is then written with
  /// `FROM_UNIXTIME()` and read with `UNIX_TIMESTAMP()`, so the stored
  /// instant does not depend on the session time zone.
  bool _unixTime(DBDriver db) {
    return failedAtTimestamp && db.getName() == Driver.mysql;
  }

  /// the `failed_at` column of a SELECT
  String _failedAtColumn(DBDriver db) {
    return _unixTime(db)
        ? 'UNIX_TIMESTAMP(failed_at) * 1000 AS failed_at'
        : 'failed_at';
  }

  /// [millis] as the `failed_at` value: the number (also for a MySQL
  /// timestamp, see [_unixTime]), or a UTC time. Both keep milliseconds
  /// only, so SQLite text compares in time order.
  Object _failedAtValue(DBDriver db, int millis) {
    if (!failedAtTimestamp || _unixTime(db)) return millis;
    return DateTime.fromMillisecondsSinceEpoch(millis, isUtc: true);
  }

  /// Throws a [StateError] when the type of `failed_at` does not match
  /// [failedAtTimestamp], so a wrong value is never written. A match or
  /// a mismatch is kept, so the type is read once per driver, and a
  /// mismatch throws on every call.
  ///
  /// When the type can not be read (eg. the connection dropped), nothing
  /// is kept and the next call reads it again. With the setting off the
  /// call goes on after a warning, as without the check. With the
  /// setting on the error goes to the caller. Without the table it
  /// checks again on the next call.
  Future<void> _checkFailedAt(DBDriver db) async {
    if (_failedAtMatches) return;
    String? mismatch = _failedAtMismatch;
    if (mismatch != null) throw StateError(mismatch);
    List<Map<String, dynamic>> rows;
    try {
      rows = await db.query(_failedAtTypeSql(db.getName()));
    } catch (error) {
      if (failedAtTimestamp) rethrow;
      QueueLog.warn(
        'Queue: can not read the type of $failedTable.failed_at, it is '
        'read again next time: $error',
      );
      return;
    }
    if (rows.isEmpty) return;
    String type = '${rows.first.values.first}'.toLowerCase();
    bool isTime = type.contains('time') || type.contains('date');
    if (isTime == failedAtTimestamp) {
      _failedAtMatches = true;
      return;
    }
    mismatch = _failedAtMismatch = isTime
        ? '$failedTable.failed_at is a $type column, but '
              'QueueConfig.failedAtTimestamp is false. Set it to true.'
        : '$failedTable.failed_at is a $type column, but '
              'QueueConfig.failedAtTimestamp is true. Change the column '
              'to a timestamp first (see the queue docs), or set it to '
              'false.';
    throw StateError(mismatch);
  }

  static String _failedAtTypeSql(Driver driver) {
    return switch (driver) {
      Driver.sqlite =>
        "SELECT type FROM pragma_table_info('$failedTable') "
            "WHERE name = 'failed_at'",
      Driver.postgres =>
        'SELECT data_type FROM information_schema.columns '
            'WHERE table_schema = current_schema() AND table_name = '
            "'$failedTable' AND column_name = 'failed_at'",
      Driver.mysql =>
        'SELECT data_type FROM information_schema.columns '
            'WHERE table_schema = DATABASE() AND table_name = '
            "'$failedTable' AND column_name = 'failed_at'",
    };
  }

  /// delete the rows of [table] that match [where], and count them
  Future<int> _deleteWhere(String table, String where, Object value) {
    DBDriver db = _db;
    Map<String, dynamic> values = <String, dynamic>{'value': value};
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

  Future<FailedJob?> _find(DBDriver db, String id) async {
    List<Map<String, dynamic>> rows = await db.query(
      'SELECT uuid, queue, payload, exception, ${_failedAtColumn(db)} '
      'FROM $failedTable WHERE uuid = @uuid',
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
      failedAt: _time(row['failed_at']),
    );
  }

  /// `failed_at`: a UTC time from a timestamp column, or unix time in
  /// milliseconds. Both give the same local [DateTime].
  static DateTime _time(Object? value) {
    if (value is DateTime) return value.toLocal();
    return DateTime.fromMillisecondsSinceEpoch(_int(value));
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
