import 'dart:async';

import 'package:dox_core/dox_core.dart';

import 'queue.dart';
import 'queue_config.dart';
import 'queue_worker.dart';
import 'worker.dart';

/// Sets up the queue in every isolate. Extend it in `lib/` so the
/// generated container (`lib/container.g.dart`) finds it:
/// ```
/// class AppQueueService extends QueueService {
///   AppQueueService() : super(queueConfig);
/// }
/// ```
/// With `QueueConfig(workInProcess: true)` it also runs a worker in the
/// first isolate of the web server.
class QueueService implements DoxService, DoxShutdown {
  QueueService(this.config);

  final QueueConfig config;

  Worker? _worker;
  Future<void>? _running;

  @override
  Future<void> setup() async {
    Queue.configure(config);
    bool inProcess =
        config.workInProcess &&
        config.driver != 'sync' &&
        !QueueWorker.active &&
        Dox().isolateId == 1;
    if (!inProcess) return;
    Worker worker = Worker(
      driver: Queue.driver,
      jobs: config.jobs,
      options: WorkerOptions(
        queues: config.workerQueues,
        concurrency: config.workerConcurrency,
      ),
    );
    _worker = worker;
    _running = worker.run();
  }

  /// stop the in-process worker (running jobs finish) and close the
  /// job store
  @override
  Future<void> shutdown() async {
    _worker?.stop();
    await _running;
    await Queue.close();
  }
}
