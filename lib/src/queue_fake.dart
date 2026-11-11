import 'job.dart';
import 'queue.dart';

/// A job that `Queue.dispatch()` got while `Queue.fake()` was on.
class PushedJob {
  const PushedJob(this.job, {required this.queue, this.delay, this.connection});

  /// the job, as it was dispatched
  final DoxJob job;

  /// the queue it would go on: `dispatch(queue:)`, else `job.queue`
  final String queue;

  /// the `dispatch(delay:)`, or null when there was none
  final Duration? delay;

  /// the `dispatch(connection:)`, or null for the main connection
  final String? connection;
}

/// Keeps every dispatched job in [pushed] and runs or saves nothing.
/// For tests. `Queue.fake()` returns it.
///
/// A failed assert throws an [AssertionError] that says what was pushed.
class QueueFake {
  /// every pushed job, oldest first
  final List<PushedJob> pushed = <PushedJob>[];

  /// every job of `dispatchSync()`, oldest first. The fake does not
  /// stop them: they still run. They are not in [pushed].
  final List<DoxJob> dispatchedSync = <DoxJob>[];

  /// every job of `dispatchAfterResponse()`, oldest first. The fake does
  /// not stop them: they still run. They are not in [pushed].
  final List<DoxJob> dispatchedAfterResponse = <DoxJob>[];

  /// every chain of `Queue.chain(...).dispatch()`, oldest first. Its
  /// jobs are not in [pushed].
  final List<PendingChain> chains = <PendingChain>[];

  /// the unique locks of [ShouldBeUnique] jobs: key and end time (null:
  /// no end). The fake has no worker, so only [clear] frees them.
  final Map<String, DateTime?> _uniqueLocks = <String, DateTime?>{};

  /// Take the unique lock [key] for [time] (null: no end). False when
  /// it is taken. `Queue.dispatch()` calls it for a [ShouldBeUnique] job.
  bool lockUnique(String key, Duration? time) {
    DateTime now = DateTime.now();
    if (_uniqueLocks.containsKey(key)) {
      DateTime? end = _uniqueLocks[key];
      if (end == null || end.isAfter(now)) return false;
    }
    _uniqueLocks[key] = time == null ? null : now.add(time);
    return true;
  }

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

  /// A chain of jobs of exactly these [types], in this order, was
  /// dispatched.
  /// ```
  /// queue.assertChained(<Type>[ResizeImage, UploadImage]);
  /// ```
  void assertChained(List<Type> types) {
    for (PendingChain chain in chains) {
      List<Type> chained = <Type>[
        for (DoxJob job in chain.jobs) job.runtimeType,
      ];
      if (_sameTypes(chained, types)) return;
    }
    String seen = chains.isEmpty
        ? 'No chains were dispatched.'
        : 'Chains: ${chains.map(_chainNames).join('; ')}.';
    throw AssertionError(
      'Expected a chain of ${types.join(', ')} to be dispatched. $seen',
    );
  }

  /// No job was pushed at all.
  void assertNothingPushed() {
    if (pushed.isNotEmpty) {
      throw AssertionError('Expected no jobs to be pushed. ${_summary()}');
    }
  }

  /// forget the pushed jobs, the [chains], the jobs of [dispatchedSync]
  /// and [dispatchedAfterResponse], and the unique locks
  void clear() {
    pushed.clear();
    chains.clear();
    _uniqueLocks.clear();
    dispatchedSync.clear();
    dispatchedAfterResponse.clear();
  }

  String _summary() {
    if (pushed.isEmpty) return 'No jobs were pushed.';
    String names = pushed.map(_name).join(', ');
    return 'Pushed: $names.';
  }

  /// names that start with a "you" sound: "Eu", "Uni", and "U" + one
  /// consonant (not "n") + vowel, like User. Unused gives "an".
  // ponytail: a small rule, not a full English dictionary (hour, honest)
  static final RegExp _youSound = RegExp(
    '^(eu|uni|u[^aeioun][aeiou])',
    caseSensitive: false,
  );

  /// `a SendWelcomeEmail`, `an AllTypesJob`, `a UserJob`, `an UnusedJob`.
  /// A vowel gives "an", but a "you" sound ([_youSound]) gives "a".
  static String _a(String name) {
    bool vowel = 'AEIOUaeiou'.contains(name.isEmpty ? 'x' : name[0]);
    return vowel && !_youSound.hasMatch(name) ? 'an $name' : 'a $name';
  }

  /// `1 time`, `2 times`
  static String _times(int count) => '$count ${_plural(count, 'time')}';

  static String _plural(int count, String word) {
    return count == 1 ? word : '${word}s';
  }

  static bool _sameTypes(List<Type> a, List<Type> b) {
    if (a.length != b.length) return false;
    for (int i = 0; i < a.length; i++) {
      if (a[i] != b[i]) return false;
    }
    return true;
  }

  static String _chainNames(PendingChain chain) {
    return chain.jobs.map(_jobName).join(', ');
  }

  static String _jobName(DoxJob job) => '${job.runtimeType}';

  static String _name(PushedJob item) => '${item.job.runtimeType}';

  static bool _isA<T>(PushedJob item) => item.job is T;

  static T _jobAs<T>(PushedJob item) => item.job as T;
}
