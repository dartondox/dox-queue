# Chains and batches

## Chains

Run jobs one after the other. The next job is pushed only when the one before it is done.

```dart
await Queue.chain(<DoxJob>[
  ResizeImage(imageId: 1),
  UploadImage(imageId: 1),
  TellUser(imageId: 1),
]).catchError(TellAdmin(imageId: 1)).dispatch(queue: 'images');
```

The catch handler is a job too (a class in `jobRegistry`). Add `CatchesError` to read the error:

```dart
@Job()
class TellAdmin extends DoxJob with CatchesError {
  TellAdmin({required this.imageId});

  final int imageId;

  @override
  Future<void> handle() async {
    await alertAdmin('image $imageId failed: $caughtError');
  }
}
```

- `dispatch()` returns the id of the first job. `queue` and `connection` count for every job. `delay` counts for the first job only.
- When a job fails for good, the rest of the chain is dropped and the catch job runs.
- A released or retried job keeps the chain. A job that ends with `delete()` counts as done.
- Keep chains short. Each job carries the data of the jobs after it. Save ids, not big data.
- Jobs in a chain can run twice (a crash between the push and the delete). Make them safe to run twice, and give them more than one try.
- A skipped `ShouldBeUnique` job stops the chain. The catch job runs with a `StateError`.
- `Queue.fake()` keeps the chain in `chains`, not in `pushed`. Check with `assertChained([A, B])`.

## Batches

```dart
Batch batch = await Queue.batch(<DoxJob>[
  ImportRows(from: 0),
  ImportRows(from: 1000),
])
    .name('import')
    .then(ImportDone())
    .catchError(ImportFailed())
    .whenComplete(CleanUpImport())
    .dispatch(queue: 'imports');
```

| Callback | Runs |
| --- | --- |
| `before(job)` | once at `dispatch()`, before the jobs are pushed |
| `progress(job)` | each time a job is done |
| `then(job)` | once, when every job is done |
| `catchError(job)` | once, when the first job fails for good |
| `whenComplete(job)` | once, when every job ran (done or failed) |

Each callback is a job class in `jobRegistry`. It runs once, like `dispatchSync()`: no queue, no retry.
Add `Batchable` to a job to read its batch, and `CatchesError` to read the error:

```dart
@Job()
class ImportRows extends DoxJob with Batchable {
  ImportRows({required this.from});

  final int from;

  @override
  Future<void> handle() async {
    Batch? batch = await this.batch();
    if (batch?.cancelled ?? false) return;
    await importRows(from);
  }
}
```

`Batch` has `id`, `name`, `totalJobs`, `pendingJobs`, `failedJobs`, `processedJobs`, `progress` (0 to 100), `finished`, `cancelled`, `cancel()` and `add(jobs)`. Read it again with `Queue.findBatch(id)`.

- The table is `job_batches` (also with the Redis driver). `dox queue:table` makes it. An older migration needs a new one that calls `QueueTables.upBatches(schema)`. Without the table, `dispatch()` throws a `StateError`.
- The first job that fails for good cancels the batch. Call `allowFailures()` to keep it going.
- `cancel()` does not stop jobs. A `Batchable` job reads `cancelled` and skips its work.
- An empty batch throws `ArgumentError`. `add(jobs)` on a finished or cancelled batch throws `StateError`.
- `dart run bin/queue.dart prune-batches --hours=48` deletes old finished batches. `--unfinished=168` also deletes batches that never finished.
- `Queue.fake()` keeps batches in `batches`. Check with `assertBatched()` and `assertBatchCount(n)`.

### A chain inside a batch

```dart
await Queue.batch(<DoxJob>[ImportUsers()])
    .chain(<DoxJob>[ImportOrders(), SumOrders()])
    .dispatch();
```

- The chain counts as one job of the batch. It is done when its last job is done.
- When a job of the chain fails for good, the rest of the chain is dropped and the batch counts one failed job. `allowFailures()` and `catchError()` work as for any job.
- `add(jobs)` takes jobs only, not chains.

### After the commit

```dart
await QueryBuilder.transaction(() async {
  Order order = await Order().create(data);
  await Queue.batch(<DoxJob>[ChargeOrder(id: order.id)])
      .chain(<DoxJob>[PackOrder(id: order.id), ShipOrder(id: order.id)])
      .dispatch(afterCommit: true);
});
```

The batch row and its jobs wait for the commit. A rollback drops them. `Queue.chain(jobs).dispatch(afterCommit: true)` works the same. Before the commit the returned `Batch` is not saved yet.

### Batch events

```dart
Queue.batchProgress((QueueEvent event) => print('${event.batch!.progress}%'));
Queue.batchFailed((QueueEvent event) => alert(event.batch!.id, event.error));
```

Also `Queue.batchDispatched`, `Queue.batchCancelled` and `Queue.batchFinished`. Each fires once. A failure without `allowFailures()` fires `batchFailed`, then `batchCancelled`.
- Test: `queue.assertBatchedChain([ImportOrders, SumOrders])`.
