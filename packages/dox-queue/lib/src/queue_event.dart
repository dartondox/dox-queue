import 'dart:async';

import 'job.dart';
import 'queue.dart' show Batch;
import 'queue_log.dart';

/// What happened, see [QueueEvent].
enum QueueEventType {
  /// `dispatch()` pushed a job (not with the `sync` driver)
  queued,

  /// a worker starts a job
  processing,

  /// a worker finished a job and deleted it (also after `delete()`)
  processed,

  /// a worker put a job back on the queue: `release()` or a retry
  released,

  /// `handle()` threw and the job will be tried again
  exceptionOccurred,

  /// a job failed for good
  failed,

  /// a worker loop is about to take the next job
  looping,

  /// a worker stops
  stopping,

  /// `Queue.batch(...).dispatch()` saved a batch, before its jobs are
  /// pushed. Fires in the process that dispatched it.
  batchDispatched,

  /// A job (or a chain) of a batch is done, or, with `allowFailures()`,
  /// failed for good. Fires in the process that counted it: the worker,
  /// or the dispatcher with the `sync` driver.
  batchProgress,

  /// Every job of a batch ran (done or failed). Fires once, in the
  /// process that counted the last job.
  batchFinished,

  /// The first job of a batch failed for good. Fires once, in the
  /// process that counted it. [QueueEvent.error] is its error.
  batchFailed,

  /// A batch was cancelled: `Batch.cancel()`, the first failure without
  /// `allowFailures()`, or a push that threw. Fires once, in the process
  /// that cancelled it.
  batchCancelled,
}

/// A listener of queue events. It may return a Future: the queue waits
/// for it. An error is logged and never stops the worker.
typedef QueueListener = FutureOr<void> Function(QueueEvent event);

/// One event of the queue. Listen with `Queue.before()`, `Queue.after()`
/// and the other hooks of `Queue`.
class QueueEvent {
  const QueueEvent(
    this.type, {
    this.job,
    this.name,
    this.id,
    this.queue,
    this.attempts = 0,
    this.error,
    this.stack,
    this.delay,
    this.batch,
  });

  final QueueEventType type;

  /// The job. Null for [QueueEventType.looping],
  /// [QueueEventType.stopping] and the batch events, and when a worker
  /// can not read the job.
  final DoxJob? job;

  /// the job name of the registry
  final String? name;

  /// the job id
  final String? id;
  final String? queue;

  /// the try number of a worker. 0 for `queued`, `looping` and `stopping`.
  final int attempts;

  /// the error of `failed` and `exceptionOccurred`
  final Object? error;
  final StackTrace? stack;

  /// the wait of `queued` and `released`
  final Duration? delay;

  /// The batch of a `batch...` event, as it was after the step: its
  /// `id`, `name` and counts. Null for the job events.
  final Batch? batch;

  static final Map<QueueEventType, List<QueueListener>> _listeners =
      <QueueEventType, List<QueueListener>>{};

  /// add [listener] for [type]
  static void listen(QueueEventType type, QueueListener listener) {
    (_listeners[type] ??= <QueueListener>[]).add(listener);
  }

  /// remove every listener
  static void forgetListeners() => _listeners.clear();

  /// Call the listeners of this event, in the order they were added. A
  /// listener that throws is logged, the next one still runs.
  Future<void> fire() async {
    List<QueueListener>? listeners = _listeners[type];
    if (listeners == null) return;
    for (QueueListener listener in List<QueueListener>.of(listeners)) {
      try {
        await listener(this);
      } catch (error, stack) {
        QueueLog.danger(
          'Queue: a ${type.name} listener threw: $error',
          '$stack',
        );
      }
    }
  }
}
