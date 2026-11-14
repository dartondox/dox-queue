## 3.2.1

No changes.

## 3.2.0

- First version. It holds the database parts that moved out of dox_queue: the `database` queue driver (`DatabaseQueueDriver` and `DatabaseQueueDriverFactory`, jobs in `jobs`, failed jobs in `failed_jobs`), the batch store for `Queue.batch()` (`DatabaseBatchRepository`, table `job_batches`), `OrmTransactions` for `dispatch(afterCommit: true)` and `QueueConfig(afterCommit: true)`, `QueueTables` (`up`, `down`, `upBatches`, `downBatches`) and the `dox queue:table` command. `DatabaseQueueProvider` turns on all three in each isolate: make your queue provider extend it in place of `QueueProvider`. `QueueDatabase.register()` does the same for a custom setup with no providers.
- Fix: with `afterCommit`, a transaction on a named dox_orm connection no longer counts as open. Only a transaction of the default connection defers the job (it is the one the job waits for).
- `QueueConfig(failedAtTimestamp: true)` keeps `failed_jobs.failed_at` as a timestamp column in UTC. See the README.
