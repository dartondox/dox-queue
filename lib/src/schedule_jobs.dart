import 'package:dox_scheduler/dox_scheduler.dart' show Schedule, ScheduledTask;

import 'job.dart';

/// Queue a job on a schedule, like Laravel `$schedule->job()`.
/// ```
/// void schedule(Schedule s) {
///   s.job(SendReport()).dailyAt('08:00');
///   s.job(PruneOldFiles(), name: 'prune', connection: 'database').hourly();
/// }
/// ```
extension ScheduleJobs on Schedule {
  /// Add a task that dispatches [job] each time it runs. [name] must be
  /// unique in the schedule. Default: the class name of the job.
  /// [connection] is the queue connection (null: the main one).
  ScheduledTask job(DoxJob job, {String? name, String? connection}) {
    _DispatchTask task = _DispatchTask(job, connection);
    return call(name ?? '${job.runtimeType}', task.run);
  }
}

/// the callback of [ScheduleJobs.job]
class _DispatchTask {
  _DispatchTask(this.job, this.connection);

  final DoxJob job;
  final String? connection;

  Future<void> run() async {
    await job.dispatch(connection: connection);
  }
}
