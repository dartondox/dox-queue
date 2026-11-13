# dox_queue_database

The database parts of [dox_queue](https://pub.dev/packages/dox_queue):
a database queue driver, the batch store for `Queue.batch()`, and
`afterCommit` for dox_orm transactions.

It works on SQLite, Postgres, MySQL and MariaDB, through the dox_orm
connection of your app.

## Install

```bash
dart pub add dox_queue_database
```

## Make the tables

```bash
dox queue:table
dox migrate
```

`dox queue:table` writes a `create_queue_tables` migration. It does not
overwrite: when the migration is already there, it prints so and stops.
The migration calls `QueueTables.up(schema)` and
`QueueTables.upBatches(schema)`. The `down` method calls `downBatches` and
`down`. You can write it by hand with these.

## Register it

Call `QueueDatabase.register()` in your queue provider, before
`super.setup()`:

```dart
import 'package:dox_queue/dox_queue.dart';
import 'package:dox_queue_database/dox_queue_database.dart';

class AppQueueProvider extends QueueProvider {
  AppQueueProvider() : super(queueConfig);

  @override
  Future<void> setup() async {
    QueueDatabase.register();
    await super.setup();
  }
}
```

This turns on all three in the isolate:

- the `database` driver (`DatabaseQueueDriver`),
- the batch store (`DatabaseBatchRepository`),
- `afterCommit` on dox_orm transactions (`OrmTransactions`).

Then use the driver in the queue config:

```dart
QueueConfig queueConfig = QueueConfig(
  driver: 'database',
  jobs: jobRegistry,
);
```

Without `register()`, `QueueConfig(driver: 'database')` throws an
`ArgumentError` that names this package.

## The tables

- `jobs`: the queued jobs. Postgres, MySQL and MariaDB workers take a job
  with `SELECT ... FOR UPDATE SKIP LOCKED`, so many workers do not wait on
  each other. A SQLite worker takes a job with one `UPDATE ... RETURNING`.
  SQLite runs one write at a time, so this is safe. It is good for small
  apps.
- `failed_jobs`: the jobs that failed for good.
- `job_batches`: the batches of `Queue.batch()`. Only batches need it.

`failed_jobs.connection` has the name of the queue connection (see
"Queue connections" in the dox_queue README). It is NULL for the main
connection. A `failed_jobs` table without this column works too. Then the
name is not saved. A worker checks for the column once, so restart the
workers after you add it. To add the column, write a migration:

```dart
await schema.table('failed_jobs', (Table table) {
  table.string('connection').nullable();
});
```

Use another connection of `config/database.dart` with
`QueueConfig(connection: 'postgres')`. Two `database` queue connections on
the same dox_orm connection share the `jobs` and `failed_jobs` tables, so
give each one its own dox_orm connection.

## Batches

`Queue.batch()` saves its batches in the `job_batches` table. This is also
so with the Redis driver. Add this package and call
`QueueDatabase.register()` for batches, also when your jobs run on Redis.
Without it, `Queue.batch()` throws a `StateError` that says to add
`dox_queue_database`.

`DatabaseBatchRepository` uses the dox_orm connection of
`QueueConfig.connection` (null: the default connection). Each count change
is one atomic statement, so with many workers each callback runs once.

An app that ran an older migration without `job_batches` adds it in a new
migration:

```dart
class CreateJobBatchesTable extends Migration {
  @override
  Future<void> up(Schema schema) async {
    await QueueTables.upBatches(schema);
  }

  @override
  Future<void> down(Schema schema) async {
    await QueueTables.downBatches(schema);
  }
}
```

## afterCommit

`dispatch(afterCommit: true)` and `QueueConfig(afterCommit: true)` need
this package. Inside a transaction, the job is pushed only after the
outermost transaction commits. A rollback drops it.

```dart
await QueryBuilder.transaction(() async {
  User user = await User().create(<String, dynamic>{'email': email});
  await SendWelcomeEmail(userId: user.id).dispatch(afterCommit: true);
});
```

Only a transaction of the default dox_orm connection defers a job. A
transaction on a named connection does not. The job is pushed at once.

Without `QueueDatabase.register()`, a dispatch with `afterCommit` on throws
a `StateError`.

## The failed_at column

`failed_jobs.failed_at` is the time the job failed. It has one of two types:

- A big integer: unix time in milliseconds. This is the default.
- A timestamp in UTC. Turn it on with `failedAtTimestamp: true`.

`FailedJob.failedAt` is the same `DateTime` with both types.

To make a new table with the timestamp column, pass the setting to `QueueTables.up` in the migration, and set it in the queue config too:

```dart
await QueueTables.up(schema, failedAtTimestamp: true);
```

```dart
QueueConfig queueConfig = QueueConfig(
  driver: 'database',
  jobs: jobRegistry,
  failedAtTimestamp: true,
);
```

The setting counts per connection. Each entry of `connections` has its own `failedAtTimestamp`, so two `database` connections can use tables of different types.

The setting and the column must match. A worker reads the column type once, when it first uses `failed_jobs`.

**What happens when the setting and the column do not match**

`fail`, `failed`, `retry`, `forget` and `prune-failed` throw a `StateError`. Nothing is written.

A job that fails all its tries is NOT moved to `failed_jobs`, and its `failed()` hook does not run. The job stays in `jobs`. It comes back after `retryAfter` and runs again, with an error in the log each time. This goes on until the setting matches the table.

When the type can not be read (for example, the connection dropped), the worker reads it again on the next call. With the setting off, it logs a warning and goes on. With the setting on, the call throws that error, and nothing is written.

The column type on each database:

| Database | Column | How the time is kept |
| --- | --- | --- |
| SQLite | `TIMESTAMP` | ISO text in UTC, with milliseconds |
| Postgres | `TIMESTAMP` (without time zone) | the UTC time, with milliseconds |
| MySQL, MariaDB | `TIMESTAMP` | whole seconds, in UTC. The queue writes it with `FROM_UNIXTIME()` and reads it with `UNIX_TIMESTAMP()`, so the session time zone does not change the time. |

A MySQL and MariaDB `TIMESTAMP` can not hold a time after 2038-01-19.

### Use the timestamp column on a table with the integer column

**Stop all workers first, also on other servers.**
A worker reads the column type when it first uses `failed_jobs`, and keeps it. A worker that is still running writes numbers into the timestamp column. On SQLite this works without an error, and then pruning and the order by time are wrong.

1. Stop all workers, on every server. Stop the app too when it has `workInProcess: true`.
2. Run the SQL for your database below, in a new migration (`await schema.driver.execute('...');` for each line) or in a database client.
3. Set `failedAtTimestamp: true` in the queue config.
4. Start the workers again.

SQLite (3.35 or newer):

```sql
ALTER TABLE failed_jobs ADD COLUMN failed_at_utc TIMESTAMP;
UPDATE failed_jobs SET failed_at_utc = strftime('%Y-%m-%dT%H:%M:%fZ', failed_at / 1000.0, 'unixepoch');
ALTER TABLE failed_jobs DROP COLUMN failed_at;
ALTER TABLE failed_jobs RENAME COLUMN failed_at_utc TO failed_at;
```

Postgres:

```sql
ALTER TABLE failed_jobs ALTER COLUMN failed_at TYPE TIMESTAMP USING to_timestamp(failed_at / 1000.0) AT TIME ZONE 'UTC';
```

MySQL and MariaDB:

```sql
ALTER TABLE failed_jobs ADD COLUMN failed_at_utc TIMESTAMP NULL;
UPDATE failed_jobs SET failed_at_utc = FROM_UNIXTIME(failed_at DIV 1000);
ALTER TABLE failed_jobs DROP COLUMN failed_at;
ALTER TABLE failed_jobs CHANGE failed_at_utc failed_at TIMESTAMP NOT NULL;
```

On SQLite the new column allows NULL, because SQLite can not add a NOT NULL column without a default. The queue never writes NULL there.
