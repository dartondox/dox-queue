## 3.2.2

- Faster: on Postgres a worker takes a job with one `UPDATE ... WHERE id = (SELECT ... FOR UPDATE SKIP LOCKED) RETURNING` statement. Before, it was a transaction with a select and an update (4 round trips). SQLite already used one statement. The job order, the attempts count and `reserved_at` are the same.
- Faster: a worker with several queues asks for all of them in one statement on Postgres and SQLite, first queue first. Each queue has its own sub-select in a `COALESCE`, which stops at the first queue with a job. So a poll never sorts the jobs of all queues. On MySQL and MariaDB the pop stays one short transaction for each queue. After a pop that found nothing, one plain read with no lock checks all queues first, so an idle worker makes one round trip, not three for each queue.
- Fix: on Postgres, right after a burst of jobs into a `jobs` table that Postgres thinks is small (a new table, or one that is often near empty), each pop read and sorted every free job. A pop took about 100 times longer, and the database CPU went up, until Postgres ran `ANALYZE` on the table (about a minute). A new `jobs` table now has an index on `(queue, id, reserved_at, available_at)`, so a pop stops at the first free job and sorts nothing.
- New: a new `jobs` table on Postgres also has an index on `(queue, reserved_at, available_at)`. With many delayed jobs a poll no longer reads them all. On SQLite the new table has only the `(queue, id, reserved_at, available_at)` index, in place of the index on `queue`: SQLite never used the other one well. An existing table does not change. Add the indexes in a new migration with `QueueTables.popIndex(schema)` (see the README). It adds only the indexes the table does not have. On MySQL and MariaDB the table keeps the index on `queue`, and `popIndex` does nothing: the pop there reads in id order already, and with the other index `FOR UPDATE` would lock every free job.
- Faster: `Queue.batch()` adds its jobs with multi-row inserts (up to 400 rows and about 1 MB per insert). When it needs more than one insert, they run in one transaction, so a failed push adds no job.
- Faster: the batch store keeps the decoded `options` of the last batch it read. The next done job of the same batch needs no JSON decode. Each read still gets its own copy of the map.
- Change: the log line of the database driver now starts with the tag `[queue]` in place of the text `Queue: `. This is only the log text. The API is the same.

## 3.2.1

No changes.

## 3.2.0

- First version. It holds the database parts that moved out of dox_queue: the `database` queue driver (`DatabaseQueueDriver` and `DatabaseQueueDriverFactory`, jobs in `jobs`, failed jobs in `failed_jobs`), the batch store for `Queue.batch()` (`DatabaseBatchRepository`, table `job_batches`), `OrmTransactions` for `dispatch(afterCommit: true)` and `QueueConfig(afterCommit: true)`, `QueueTables` (`up`, `down`, `upBatches`, `downBatches`) and the `dox queue:table` command. `DatabaseQueueProvider` turns on all three in each isolate: make your queue provider extend it in place of `QueueProvider`. `QueueDatabase.register()` does the same for a custom setup with no providers.
- Fix: with `afterCommit`, a transaction on a named dox_orm connection no longer counts as open. Only a transaction of the default connection defers the job (it is the one the job waits for).
- `QueueConfig(failedAtTimestamp: true)` keeps `failed_jobs.failed_at` as a timestamp column in UTC. See the README.
