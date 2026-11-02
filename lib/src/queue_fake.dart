import 'job.dart';

/// A job that `Queue.dispatch()` got while `Queue.fake()` was on.
class PushedJob {
  const PushedJob(this.job, {required this.queue, this.delay});

  /// the job, as it was dispatched
  final DoxJob job;

  /// the queue it would go on: `dispatch(queue:)`, else `job.queue`
  final String queue;

  /// the `dispatch(delay:)`, or null when there was none
  final Duration? delay;
}

/// Keeps every dispatched job in [pushed] and runs or saves nothing.
/// For tests. `Queue.fake()` returns it.
///
/// A failed assert throws an [AssertionError] that says what was pushed.
class QueueFake {
  /// every pushed job, oldest first
  final List<PushedJob> pushed = <PushedJob>[];

  /// the pushed jobs of type [T]
  List<PushedJob> pushedOf<T extends DoxJob>() {
    return pushed.where(_isA<T>).toList();
  }

  /// the pushed jobs of type [T], as [T]
  List<T> jobsOf<T extends DoxJob>() {
    return pushedOf<T>().map(_jobAs<T>).toList();
  }

  /// A job of type [T] was pushed. With [test], one of them must pass it.
  void assertPushed<T extends DoxJob>([bool Function(T job)? test]) {
    List<T> jobs = jobsOf<T>();
    if (jobs.isEmpty) {
      throw AssertionError('Expected a $T job to be pushed. ${_summary()}');
    }
    if (test != null && !jobs.any(test)) {
      throw AssertionError(
        'Expected a $T job that passes the test to be pushed, but none of '
        'the ${jobs.length} pushed $T jobs passed it.',
      );
    }
  }

  /// Exactly [times] jobs of type [T] were pushed.
  void assertPushedTimes<T extends DoxJob>(int times) {
    int count = pushedOf<T>().length;
    if (count != times) {
      throw AssertionError(
        'Expected $T to be pushed $times times, but it was pushed '
        '$count times.',
      );
    }
  }

  /// No job of type [T] was pushed.
  void assertNotPushed<T extends DoxJob>() {
    int count = pushedOf<T>().length;
    if (count > 0) {
      throw AssertionError(
        'Expected no $T job to be pushed, but it was pushed $count times.',
      );
    }
  }

  /// No job was pushed at all.
  void assertNothingPushed() {
    if (pushed.isNotEmpty) {
      throw AssertionError('Expected no jobs to be pushed. ${_summary()}');
    }
  }

  /// forget the pushed jobs
  void clear() => pushed.clear();

  String _summary() {
    if (pushed.isEmpty) return 'No jobs were pushed.';
    String names = pushed.map(_name).join(', ');
    return 'Pushed: $names.';
  }

  static String _name(PushedJob item) => '${item.job.runtimeType}';

  static bool _isA<T>(PushedJob item) => item.job is T;

  static T _jobAs<T>(PushedJob item) => item.job as T;
}
