import 'package:dox_orm/dox_orm.dart';
import 'package:dox_queue/dox_queue.dart';

import 'database_batch_repository.dart';
import 'database_queue_driver.dart';

/// Turns on the database parts of dox_queue in this isolate:
/// - the `database` driver (`QueueConfig(driver: 'database')`),
/// - the `job_batches` store of `Queue.batch()`,
/// - `dispatch(afterCommit: true)` on dox_orm transactions.
///
/// Call [register] in `setup()` of the queue provider, before
/// `super.setup()`:
/// ```
/// class AppQueueProvider extends QueueProvider {
///   AppQueueProvider() : super(queueConfig);
///
///   @override
///   Future<void> setup() async {
///     QueueDatabase.register();
///     await super.setup();
///   }
/// }
/// ```
class QueueDatabase {
  /// Safe to call more than once.
  static void register() {
    Queue.extend('database', const DatabaseQueueDriverFactory());
    Queue.useBatches(const DatabaseBatchRepository());
    Queue.useTransactions(const OrmTransactions());
  }
}

/// The transactions of the default dox_orm connection, for
/// `dispatch(afterCommit: true)`.
class OrmTransactions implements QueueTransactions {
  const OrmTransactions();

  /// True only inside a transaction of the default connection: that is
  /// the one [afterCommit] waits for. A transaction of a named
  /// connection does not count.
  // ponytail: waits for the default dox_orm connection only
  @override
  bool get inTransaction => SqlQueryBuilder().getDBDriver().hasOpenTransaction;

  @override
  Future<void> afterCommit(Future<void> Function() callback) {
    return QueryBuilder.afterCommit(callback);
  }
}
