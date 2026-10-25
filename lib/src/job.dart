import 'dart:async';

import 'queue.dart';

/// Marks a background job. `dart run build_runner build` finds every
/// `@Job()` class in `lib/` and writes `lib/generated/jobs.g.dart`.
///
/// [name] is the name saved with each queued job. Default: the class
/// name. Set it when you rename the class while jobs of the old name
/// are still in the queue.
/// ```
/// @Job()
/// class SendWelcomeEmail extends DoxJob { ... }
/// ```
class Job {
  const Job({this.name});

  final String? name;
}

/// A background job. Put `@Job()` above the class.
///
/// The parameters of the unnamed constructor are saved with the job.
/// Each one needs a field or getter of the same name. Their types must
/// be JSON types: `int`, `double`, `num`, `String`, `bool`, `DateTime`,
/// an enum, or a `List` or `Map<String, ...>` of these.
///
/// Get services inside [handle] from the Dox container:
/// `Dox().ioc.get<UserService>()`.
/// ```
/// @Job()
/// class SendWelcomeEmail extends DoxJob {
///   SendWelcomeEmail({required this.userId});
///
///   final int userId;
///
///   @override
///   int get tries => 3;
///
///   @override
///   Future<void> handle() async {
///     UserService users = Dox().ioc.get<UserService>();
///     await users.sendWelcomeEmail(userId);
///   }
/// }
///
/// await SendWelcomeEmail(userId: 1).dispatch();
/// ```
abstract class DoxJob {
  const DoxJob();

  /// The work of the job. When it throws, the worker tries again (see
  /// [tries] and [backoff]). After the last try it calls [failed].
  Future<void> handle();

  /// How many times the worker runs the job before it fails.
  /// Null: the worker `--tries` value (default 1, so no retry).
  int? get tries => null;

  /// The wait before each retry. The first entry is the wait before the
  /// second try, and so on. The last entry is used for the rest.
  /// Empty: retry at once.
  List<Duration> get backoff => const <Duration>[];

  /// The max run time of one try. Null: the worker `--timeout` value
  /// (default 60 seconds). Dart can not stop a running future, so after
  /// a timeout the try counts as failed, but [handle] keeps running in
  /// the background until it ends.
  Duration? get timeout => null;

  /// The queue to put the job on. `dispatch(queue:)` wins over it.
  String get queue => 'default';

  /// Runs once, after the last try failed. Use it to clean up or to
  /// tell someone.
  FutureOr<void> failed(Object error, StackTrace stack) {}

  /// Put the job on the queue. A worker runs it later. Returns the job
  /// id. With the `sync` driver it runs at once.
  /// ```
  /// await SendWelcomeEmail(userId: 1).dispatch();
  /// await SendWelcomeEmail(userId: 1).dispatch(
  ///   delay: Duration(minutes: 5),
  ///   queue: 'emails',
  /// );
  /// ```
  Future<String> dispatch({Duration? delay, String? queue}) {
    return Queue.dispatch(this, delay: delay, queue: queue);
  }

  /// Run the job now, in this process, with no queue.
  Future<void> dispatchSync() {
    return Queue.dispatchSync(this);
  }
}
