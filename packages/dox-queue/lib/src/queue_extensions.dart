import 'queue.dart';
import 'queue_config.dart';
import 'queue_driver.dart';

/// Makes the job store of a queue driver that dox_queue does not have.
/// Register it with `Queue.extend()`.
/// ```
/// Queue.extend('database', DatabaseQueueDriverFactory());
/// ```
abstract class QueueDriverFactory {
  /// The job store of the queue connection [config]. [name] is the name
  /// of the connection in `QueueConfig.connections` (null: the main
  /// connection).
  QueueDriver create(QueueConfig config, {String? name});
}

/// Keeps the batches of `Queue.batch()`. Set it with
/// `Queue.useBatches()`. dox_queue_database has one on the
/// `job_batches` table.
///
/// Each count change must be one atomic step, so only one worker sees a
/// count reach its end.
abstract class BatchRepository {
  /// Save the new [batch].
  Future<void> create(Batch batch);

  /// The batch [id] as it is now, or null when there is none.
  Future<Batch?> find(String id);

  /// Add the numbers to the counts of the batch [id] and return the new
  /// batch. `failedJobs` never goes below 0. Null when there is no such
  /// batch, or with [onlyOpen] when it is finished or cancelled (then
  /// nothing changes).
  Future<Batch?> change(
    String id, {
    int total = 0,
    int pending = 0,
    int failed = 0,
    bool onlyOpen = false,
  });

  /// Set the finish time of the batch [id] to [at], when it has none.
  Future<void> finish(String id, DateTime at);

  /// Cancel the batch [id] at [at], and finish it at [at] when it has no
  /// finish time. Returns the new batch. Null when it was cancelled
  /// before, or is gone: then nothing changes.
  Future<Batch?> cancel(String id, DateTime at);

  /// Delete the batches that finished before [before]. With
  /// [unfinishedBefore], also delete the batches that never finished and
  /// were made before it. Returns how many were deleted.
  Future<int> prune(DateTime before, {DateTime? unfinishedBefore});
}

/// The database transactions of `dispatch(afterCommit: true)`. Set it
/// with `Queue.useTransactions()`. dox_queue_database has the one of
/// dox_orm.
abstract class QueueTransactions {
  /// true inside an open transaction
  bool get inTransaction;

  /// Run [callback] after the open transaction commits. A rollback
  /// drops it.
  Future<void> afterCommit(Future<void> Function() callback);
}
