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
      throw AssertionError(
        'Expected ${_a('$T')} job to be pushed. ${_summary()}',
      );
    }
    if (test != null && !jobs.any(test)) {
      throw AssertionError(
        'Expected ${_a('$T')} job that passes the test to be pushed, but '
        'none of the ${jobs.length} pushed $T ${_plural(jobs.length, 'job')} '
        'passed it.',
      );
    }
  }

  /// Exactly [times] jobs of type [T] were pushed.
  void assertPushedTimes<T extends DoxJob>(int times) {
    int count = pushedOf<T>().length;
    if (count != times) {
      throw AssertionError(
        'Expected $T to be pushed ${_times(times)}, but it was pushed '
        '${_times(count)}.',
      );
    }
  }

  /// No job of type [T] was pushed.
  void assertNotPushed<T extends DoxJob>() {
    int count = pushedOf<T>().length;
    if (count > 0) {
      throw AssertionError(
        'Expected no $T job to be pushed, but it was pushed '
        '${_times(count)}.',
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

  /// `a SendWelcomeEmail`, `an AllTypesJob`, `a UserJob`. A vowel gives
  /// "an", but "U" + consonant + vowel (User, Uni) and "Eu" sound like
  /// "you", so they give "a".
  // ponytail: a small rule, not a full English dictionary (hour, honest)
  static final RegExp _youSound = RegExp(
    '^(eu|u[^aeiou][aeiou])',
    caseSensitive: false,
  );

  static String _a(String name) {
    bool vowel = 'AEIOUaeiou'.contains(name.isEmpty ? 'x' : name[0]);
    return vowel && !_youSound.hasMatch(name) ? 'an $name' : 'a $name';
  }

  /// `1 time`, `2 times`
  static String _times(int count) => '$count ${_plural(count, 'time')}';

  static String _plural(int count, String word) {
    return count == 1 ? word : '${word}s';
  }

  static String _name(PushedJob item) => '${item.job.runtimeType}';

  static bool _isA<T>(PushedJob item) => item.job is T;

  static T _jobAs<T>(PushedJob item) => item.job as T;
}
