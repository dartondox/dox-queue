import 'dart:async';

import 'job_run.dart';
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
  ///
  /// With the [InteractsWithQueue] mixin, the job can call `attempts()`,
  /// `release()`, `fail()` and `delete()` inside it.
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
  /// the background until it ends. The next try can then run at the
  /// same time. The worker `--timeout-grace` flag makes the worker wait
  /// for the old run first.
  Duration? get timeout => null;

  /// Opt-in: retry the job until this time, not [tries] times. It is
  /// read once, when the job is dispatched, and saved with the job. Like
  /// Laravel `retryUntil`. After a failed try, the job fails when this
  /// time has passed, else it is tried again after its [backoff].
  /// ```
  /// @override
  /// DateTime? get retryUntil => DateTime.now().add(Duration(minutes: 10));
  /// ```
  DateTime? get retryUntil => null;

  /// Opt-in: fail the job after this many tries that threw an error,
  /// even when [tries] or [retryUntil] allow more. A timeout is not
  /// counted. Null: no limit.
  int? get maxExceptions => null;

  /// Opt-in: fail the job at once when a try times out, with no retry.
  bool get failOnTimeout => false;

  /// The queue to put the job on. `dispatch(queue:)` wins over it.
  String get queue => 'default';

  /// Runs once, after the last try failed. Use it to clean up or to
  /// tell someone.
  FutureOr<void> failed(Object error, StackTrace stack) {}

  /// Put the job on the queue. A worker runs it later. Returns the job
  /// id. With the `sync` driver it runs at once.
  ///
  /// Wait with [delay], or until the time [delayUntil]. Not both.
  /// ```
  /// await SendWelcomeEmail(userId: 1).dispatch();
  /// await SendWelcomeEmail(userId: 1).dispatch(
  ///   delay: Duration(minutes: 5),
  ///   queue: 'emails',
  /// );
  /// await SendWelcomeEmail(userId: 1).dispatch(
  ///   delayUntil: DateTime.utc(2030, 1, 1, 9),
  /// );
  /// ```
  ///
  /// Opt-in [connection]: a name of `QueueConfig.connections`, like
  /// Laravel `onConnection('redis')`. Null: the main connection.
  ///
  /// Opt-in [afterCommit]: inside a dox_orm transaction, push the job
  /// only after it commits. See `Queue.dispatch`.
  Future<String> dispatch({
    Duration? delay,
    String? queue,
    DateTime? delayUntil,
    String? connection,
    bool? afterCommit,
  }) {
    return Queue.dispatch(
      this,
      delay: delay,
      queue: queue,
      delayUntil: delayUntil,
      connection: connection,
      afterCommit: afterCommit,
    );
  }

  /// [dispatch] when [condition] is true. Returns the job id, or null
  /// when the job was not dispatched.
  /// ```
  /// await SendWelcomeEmail(userId: 1).dispatchIf(user.wantsEmail);
  /// ```
  Future<String?> dispatchIf(
    bool condition, {
    Duration? delay,
    String? queue,
    DateTime? delayUntil,
    String? connection,
    bool? afterCommit,
  }) async {
    if (!condition) return null;
    return dispatch(
      delay: delay,
      queue: queue,
      delayUntil: delayUntil,
      connection: connection,
      afterCommit: afterCommit,
    );
  }

  /// [dispatch] when [condition] is false. Returns the job id, or null
  /// when the job was not dispatched.
  Future<String?> dispatchUnless(
    bool condition, {
    Duration? delay,
    String? queue,
    DateTime? delayUntil,
    String? connection,
    bool? afterCommit,
  }) {
    return dispatchIf(
      !condition,
      delay: delay,
      queue: queue,
      delayUntil: delayUntil,
      connection: connection,
      afterCommit: afterCommit,
    );
  }

  /// Run the job now, in this process, with no queue.
  Future<void> dispatchSync() {
    return Queue.dispatchSync(this);
  }

  /// Run the job in this process after the response is sent, like
  /// Laravel `dispatchAfterResponse`. The client does not wait for it.
  /// The job does not go on the queue, so there is no job id, no retry
  /// and no worker. It is like [dispatchSync], only later.
  ///
  /// Outside a request (eg. in a command) the job runs soon, after the
  /// code that runs now. When it throws, [failed] runs and the error
  /// goes to the exception handler. It never reaches the client.
  /// `Queue.fake()` keeps it in `dispatchedAfterResponse`, and it still
  /// runs, like [dispatchSync].
  ///
  /// It needs `AfterResponse.enable()` of dox_core. `QueueProvider`
  /// calls it. When the server stops, a job that did not run yet is
  /// lost. Use [dispatch] for work that must run.
  /// ```
  /// SendWelcomeEmail(userId: 1).dispatchAfterResponse();
  /// ```
  void dispatchAfterResponse() {
    Queue.dispatchAfterResponse(this);
  }
}

/// Opt-in: save the job data encrypted, so a person who can read Redis
/// or the `jobs` table can not read it. Like Laravel `ShouldBeEncrypted`.
/// ```
/// @Job()
/// class ChargeCard extends DoxJob with ShouldBeEncrypted { ... }
/// ```
/// The data is encrypted with AES-GCM and the app key (`AppConfig.appKey`,
/// see `AESEncryptor` of dox_core). The worker needs `Dox().initialize()`
/// with the same key, or one of `AppConfig.previousKeys`. The job name,
/// queue and id stay plain text.
mixin ShouldBeEncrypted on DoxJob {}

/// Opt-in: lets a job control its own run inside `handle()`, like
/// Laravel `InteractsWithQueue`: [attempts], [release], [fail] and
/// [delete]. Jobs without it work as before.
/// ```
/// @Job()
/// class SendReport extends DoxJob with InteractsWithQueue { ... }
/// ```
/// The worker does the call when `handle()` ends (returns or throws).
/// The last call wins. When `handle()` times out, the call is dropped
/// and the timeout rules count.
///
/// In a test file, `package:test` has a top-level `fail(message)`. Inside
/// a job class with this mixin, a bare `fail(...)` calls the job method.
/// Write `this.fail(...)` to make clear which one you mean.
mixin InteractsWithQueue on DoxJob {
  /// How many times a worker took this job, this time included. The
  /// first run gives 1. Outside a worker (eg. `dispatchSync()` or the
  /// `sync` driver) it is always 1.
  int attempts() => JobRun.of(this)?.attempts ?? 1;

  /// Opt-in: put the job back on its queue, to run again after [delay].
  /// The worker does it when `handle()` ends. It does not call `failed()`.
  /// The next run counts as a new try, so it uses up `tries`. The
  /// default `tries` is 1: then the next pickup fails the job. Set
  /// `tries` above 1 (or `retryUntil`) when a job calls `release()`.
  /// Outside a worker it does nothing.
  /// ```
  /// if (await api.isBusy()) {
  ///   release(Duration(seconds: 30));
  ///   return;
  /// }
  /// ```
  void release([Duration delay = Duration.zero]) {
    JobRun? run = JobRun.of(this);
    if (run == null) return;
    run.action = JobAction.release;
    run.delay = delay;
  }

  /// Opt-in: fail the job now, with no retry. When `handle()` ends, the
  /// worker moves it to the failed jobs and calls `failed()` with [error].
  /// No [error]: a [StateError] that says the job called `fail()`.
  /// Outside a worker (`dispatchSync()`, the `sync` driver) only
  /// `failed()` runs, and nothing is thrown.
  void fail([Object? error]) {
    JobRun? run = JobRun.of(this);
    if (run == null) return;
    run.action = JobAction.fail;
    run.error = error ?? StateError('$runtimeType called fail().');
    run.stack = StackTrace.current;
  }

  /// Opt-in: delete the job from the queue when `handle()` ends, with no
  /// retry and no `failed()` call. Outside a worker it does nothing.
  void delete() {
    JobRun.of(this)?.action = JobAction.delete;
  }
}
