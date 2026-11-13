import 'dart:async';

import 'package:dox_core/dox_core.dart';

import 'queue.dart';
import 'queue_config.dart';
import 'queue_driver.dart';
import 'queue_worker.dart';
import 'worker.dart';

/// Sets up the queue in every isolate. Extend it in `lib/` so the
/// generated container (`lib/container.g.dart`) finds it:
/// ```
/// class AppQueueProvider extends QueueProvider {
///   AppQueueProvider() : super(queueConfig);
/// }
/// ```
/// With `QueueConfig(workInProcess: true)` it also runs a worker in the
/// first isolate of the web server, in [boot]. It also turns on `AfterResponse` of
/// dox_core, for `dispatchAfterResponse()`.
class QueueProvider extends DoxProvider {
  QueueProvider(this.config);

  final QueueConfig config;

  Worker? _worker;
  QueueDriver? _driver;
  Future<void>? _running;

  @override
  Future<void> setup() async {
    Queue.configure(config);
    // for DoxJob.dispatchAfterResponse()
    AfterResponse.enable();
  }

  /// Starts the in-process worker. It runs after `setup()` of every
  /// provider, so the other providers (eg. the mail jobs of dox_mailer)
  /// are ready before the first job runs.
  @override
  Future<void> boot() async {
    bool inProcess =
        config.workInProcess &&
        config.driver != 'sync' &&
        !QueueWorker.active &&
        Dox().isolateId == 1;
    if (!inProcess) return;
    // its own job store, from this config: another provider may call
    // Queue.configure() between setup() and boot()
    QueueDriver driver = config.createDriver();
    _driver = driver;
    Worker worker = Worker(
      driver: driver,
      jobs: config.jobs,
      options: WorkerOptions(
        queues: config.workerQueues,
        concurrency: config.workerConcurrency,
        timeoutGrace: config.workerTimeoutGrace,
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
    await _driver?.close();
    _driver = null;
    await Queue.close();
  }
}
