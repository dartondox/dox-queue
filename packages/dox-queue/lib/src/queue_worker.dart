import 'dart:async';
import 'dart:io';

import 'package:dox_core/dox_core.dart';

import 'queue.dart';
import 'queue_driver.dart';
import 'worker.dart';

/// The queue command line, for the app file `bin/queue.dart`:
/// ```
/// Future<void> main(List<String> args) async {
///   exitCode = await QueueWorker(app: appConfig).run(args);
/// }
/// ```
/// It starts the app providers (database, cache, queue), but not the
/// http server. Then it runs one command:
/// - `work` (default): run jobs. Flags: `--queue=a,b`,
///   `--concurrency=N`, `--once`, `--tries=N`, `--timeout=SECONDS`,
///   `--sleep=SECONDS`, `--stop-when-empty`, `--timeout-grace=SECONDS`,
///   `--backoff=SECONDS[,SECONDS...]`, `--max-jobs=N`,
///   `--max-time=SECONDS`, `--memory=MB`.
///
/// Every command takes the opt-in flag `--connection=NAME`: use that
/// queue connection of `QueueConfig.connections`. Default: the main one.
/// - `failed`: list the failed jobs.
/// - `retry <id|all>`: put failed jobs back on their queue.
/// - `forget <id>`: delete one failed job.
/// - `flush`: delete all failed jobs.
/// - `clear [queue]`: delete the jobs of a queue (default: `default`).
/// - `restart`: tell the running workers to stop after their current
///   jobs, so the process manager starts them with the new code.
/// - `prune-failed`: delete failed jobs older than `--hours=N` (24).
/// - `prune-batches`: delete the batches of `Queue.batch()` that
///   finished more than `--hours=N` (24) ago. Opt-in `--unfinished=N`:
///   also delete the batches that never finished and were made more
///   than N hours ago.
///
/// SIGINT (Ctrl+C) and SIGTERM stop the worker gracefully: it takes no
/// new job and waits for the running jobs. A second signal stops it at
/// once.
class QueueWorker {
  QueueWorker({required this.app});

  final AppConfig app;

  /// true while a [QueueWorker] runs in this process. `QueueProvider`
  /// then does not start the in-process worker.
  static bool active = false;

  /// The cache key of `restart`. A worker reads it when it starts, and
  /// stops when the value changes. It uses the default cache store of
  /// the app, so all workers must share it (eg. `file` on one machine,
  /// or `redis`). The `memory` store does not work for it.
  static const String restartKey = 'dox_queue:restart';

  /// how often a running worker reads [restartKey]
  static const Duration restartCheck = Duration(seconds: 3);

  static const String usage = '''
Usage: dart run bin/queue.dart <command> [flags]

Commands:
  work              run jobs (default)
  failed            list the failed jobs
  retry <id|all>    put failed jobs back on their queue
  forget <id>       delete one failed job
  flush             delete all failed jobs
  clear [queue]     delete the jobs of a queue (default: default)
  restart           stop the running workers after their current jobs
  prune-failed      delete failed jobs older than --hours=N (default: 24)
  prune-batches     delete batches that finished more than --hours=N ago
                    (default: 24). --unfinished=N also deletes batches that
                    never finished, made more than N hours ago

Flags of all commands:
  --connection=NAME   the queue connection of QueueConfig.connections
                      (default: the main connection)

Flags of work:
  --queue=a,b         the queues to work, first one first (default: default)
  --concurrency=N     jobs at once in this process (default: 1)
  --once              run one job, then stop
  --tries=N           tries of a job that does not set tries (default: 1)
  --timeout=SECONDS   max run time of a job (default: 60)
  --sleep=SECONDS     the wait when the queues are empty (default: 3)
  --stop-when-empty   stop when the queues are empty
  --timeout-grace=SECONDS
                      after a timeout, wait up to this long for the old run
                      to end before the next try (default: no wait)
  --backoff=SECONDS[,SECONDS...]
                      the wait before a retry of a job with no backoff
                      (default: retry at once)
  --max-jobs=N        stop after N jobs (default: no limit)
  --max-time=SECONDS  stop after this time (default: no limit)
  --memory=MB         stop when the process uses more memory (default: no limit)''';

  /// Run the command of [args]. Returns the exit code: 0 for success.
  Future<int> run(List<String> args) async {
    QueueCommand command;
    try {
      command = QueueCommand.parse(args);
    } on FormatException catch (error) {
      stderr.writeln(error.message);
      stderr.writeln(usage);
      return 64;
    }
    active = true;
    Dox().initialize(app);
    List<DoxProvider> providers = _providers();
    try {
      for (DoxProvider provider in providers) {
        await provider.setup();
      }
      for (DoxProvider provider in providers) {
        await provider.boot();
      }
      return await _run(command);
    } catch (error) {
      stderr.writeln('Queue: $error');
      return 1;
    } finally {
      await _shutdown(providers);
      active = false;
    }
  }

  Future<int> _run(QueueCommand command) async {
    switch (command.name) {
      case 'work':
        // same check as the web server: a short or public key is easy to guess
        Dox().checkAppKey();
        return _work(command.options, command.connection);
      case 'failed':
        _printFailed(await Queue.driverOf(command.connection).failed());
        return 0;
      case 'retry':
        return _retry(Queue.driverOf(command.connection), command.argument);
      case 'forget':
        bool found = await Queue.driverOf(command.connection)
            .forget(command.argument);
        stdout.writeln(found ? 'Deleted failed job.' : 'No failed job found.');
        return found ? 0 : 1;
      case 'flush':
        await Queue.driverOf(command.connection).flush();
        stdout.writeln('Deleted all failed jobs.');
        return 0;
      case 'clear':
        String queue = command.argument.isEmpty ? 'default' : command.argument;
        int count = await Queue.driverOf(command.connection).clear(queue);
        stdout.writeln('Deleted $count jobs of the $queue queue.');
        return 0;
      case 'restart':
        await Cache().forever(restartKey, DateTime.now().toIso8601String());
        stdout.writeln('The workers stop after their current jobs.');
        return 0;
      case 'prune-failed':
        DateTime before = DateTime.now().subtract(
          Duration(hours: command.hours),
        );
        int count = await Queue.driverOf(command.connection)
            .pruneFailed(before);
        stdout.writeln('Deleted $count failed jobs.');
        return 0;
      case 'prune-batches':
        DateTime before = DateTime.now().subtract(
          Duration(hours: command.hours),
        );
        int? unfinished = command.unfinished;
        int count = await Queue.pruneBatches(
          before,
          unfinishedBefore: unfinished == null
              ? null
              : DateTime.now().subtract(Duration(hours: unfinished)),
        );
        stdout.writeln('Deleted $count batches.');
        return 0;
    }
    return 64;
  }

  Future<int> _work(WorkerOptions options, String? connection) async {
    String driverName = Queue.config.connectionOf(connection).driver;
    if (driverName == 'sync') {
      stdout.writeln(
        'The sync driver runs jobs on dispatch. No worker needed.',
      );
      return 0;
    }
    Worker worker = Worker(
      driver: Queue.driverOf(connection),
      jobs: Queue.config.jobs,
      options: options,
    );
    List<StreamSubscription<ProcessSignal>> signals =
        <StreamSubscription<ProcessSignal>>[];

    /// the first signal stops gracefully. Then stop watching: the next
    /// signal does the default, it ends the process.
    void onSignal(ProcessSignal signal) {
      Logger.info('Queue: $signal, stopping after the running jobs');
      for (StreamSubscription<ProcessSignal> s in signals) {
        s.cancel();
      }
      worker.stop();
    }

    // `restart` changes the key: stop like on a signal
    Object? restartedAt;
    bool canRestart = true;
    try {
      restartedAt = await Cache().get(restartKey);
    } catch (error) {
      canRestart = false;
      Logger.warn('Queue: no `restart`, the cache can not be read: $error');
    }
    bool checking = false;
    Future<void> checkRestart(Timer timer) async {
      if (checking) return;
      checking = true;
      try {
        if (await Cache().get(restartKey) == restartedAt) return;
      } catch (_) {
        return; // a cache error is not a restart, try again next time
      } finally {
        checking = false;
      }
      Logger.info('Queue: restart, stopping after the running jobs');
      timer.cancel();
      worker.stop();
    }

    Timer? restart = canRestart
        ? Timer.periodic(restartCheck, checkRestart)
        : null;
    signals.add(ProcessSignal.sigint.watch().listen(onSignal));
    // Windows can not watch SIGTERM
    if (!Platform.isWindows) {
      signals.add(ProcessSignal.sigterm.watch().listen(onSignal));
    }
    Logger.info(
      'Queue: worker started on ${options.queues.join(', ')} '
      '(${connection == null ? '' : '$connection, '}$driverName, '
      'concurrency ${options.concurrency})',
    );
    await worker.run();
    restart?.cancel();
    for (StreamSubscription<ProcessSignal> s in signals) {
      await s.cancel();
    }
    Logger.info('Queue: worker stopped');
    return 0;
  }

  Future<int> _retry(QueueDriver driver, String id) async {
    if (id != 'all') {
      bool found = await driver.retry(id);
      stdout.writeln(found ? 'Retried failed job.' : 'No failed job found.');
      return found ? 0 : 1;
    }
    int count = 0;
    for (FailedJob job in await driver.failed()) {
      if (await driver.retry(job.id)) count++;
    }
    stdout.writeln('Retried $count failed jobs.');
    return 0;
  }

  void _printFailed(List<FailedJob> jobs) {
    if (jobs.isEmpty) {
      stdout.writeln('No failed jobs.');
      return;
    }
    for (FailedJob job in jobs) {
      String error = job.error.split('\n').first;
      stdout.writeln(
        '${job.id}  ${job.failedAt.toIso8601String()}  ${job.queue}  '
        '${job.job}  $error',
      );
    }
  }

  /// the providers of the app config, then the ones of the container,
  /// like the web server
  List<DoxProvider> _providers() {
    List<DoxProvider> providers = List<DoxProvider>.of(app.providers);
    Set<Type> listed = <Type>{};
    for (DoxProvider provider in providers) {
      listed.add(provider.runtimeType);
      Dox().ioc.adopt(provider);
    }
    for (DoxProvider provider in Dox().ioc.providers) {
      if (listed.add(provider.runtimeType)) providers.add(provider);
    }
    return providers;
  }

  /// close what the providers opened, so the process can end
  Future<void> _shutdown(List<DoxProvider> providers) async {
    for (DoxProvider provider in providers) {
      try {
        await provider.shutdown();
      } catch (error) {
        stderr.writeln('Queue: shutdown of ${provider.runtimeType}: $error');
      }
    }
    await Queue.close();
    await Dox().cache.close();
  }
}

/// A parsed `bin/queue.dart` command line.
class QueueCommand {
  QueueCommand(
    this.name,
    this.argument,
    this.options, {
    this.hours = 24,
    this.connection,
    this.unfinished,
  });

  /// `work`, `failed`, `retry`, `forget`, `flush`, `clear`, `restart`,
  /// `prune-failed` or `prune-batches`
  final String name;

  /// the id of `retry` and `forget`, the queue of `clear`
  final String argument;
  final WorkerOptions options;

  /// `--hours` of `prune-failed` and `prune-batches`
  final int hours;

  /// `--unfinished` of `prune-batches`, in hours. Null: not given.
  final int? unfinished;

  /// `--connection`: a name of `QueueConfig.connections`. Null: the
  /// main connection.
  final String? connection;

  static const List<String> _commands = <String>[
    'work',
    'failed',
    'retry',
    'forget',
    'flush',
    'clear',
    'restart',
    'prune-failed',
    'prune-batches',
  ];

  /// Throws a [FormatException] with a clear message for a bad command
  /// line.
  static QueueCommand parse(List<String> args) {
    List<String> words = <String>[];
    Map<String, String> flags = <String, String>{};
    for (String arg in args) {
      if (!arg.startsWith('--')) {
        words.add(arg);
        continue;
      }
      int equals = arg.indexOf('=');
      String key = equals < 0 ? arg.substring(2) : arg.substring(2, equals);
      flags[key] = equals < 0 ? '' : arg.substring(equals + 1);
    }
    String name = words.isEmpty ? 'work' : words.first;
    if (!_commands.contains(name)) {
      throw FormatException('Unknown command: $name');
    }
    String argument = words.length > 1 ? words[1] : '';
    if ((name == 'retry' || name == 'forget') && argument.isEmpty) {
      throw FormatException('$name needs a job id');
    }
    for (String key in flags.keys) {
      if (!_flags.contains(key)) throw FormatException('Unknown flag: --$key');
    }
    String? queues = flags['queue'];
    return QueueCommand(
      name,
      argument,
      WorkerOptions(
        queues: queues == null || queues.isEmpty
            ? const <String>['default']
            : queues.split(',').map(_trim).toList(),
        concurrency: _int(flags, 'concurrency') ?? 1,
        once: flags.containsKey('once'),
        tries: _int(flags, 'tries') ?? 1,
        timeout: Duration(seconds: _int(flags, 'timeout') ?? 60),
        sleep: Duration(seconds: _int(flags, 'sleep') ?? 3),
        stopWhenEmpty: flags.containsKey('stop-when-empty'),
        timeoutGrace: _seconds(flags, 'timeout-grace'),
        backoff: _backoff(flags['backoff']),
        maxJobs: _int(flags, 'max-jobs'),
        maxTime: _seconds(flags, 'max-time'),
        memory: _int(flags, 'memory'),
      ),
      hours: _int(flags, 'hours') ?? 24,
      connection: _connection(flags['connection']),
      unfinished: _int(flags, 'unfinished'),
    );
  }

  /// `--connection=NAME`. Null when not given.
  static String? _connection(String? value) {
    if (value == null) return null;
    if (value.trim().isEmpty) {
      throw FormatException('--connection needs a connection name');
    }
    return value.trim();
  }

  /// `--backoff=1,5,10`: seconds, one entry per retry
  static List<Duration> _backoff(String? value) {
    if (value == null) return const <Duration>[];
    List<Duration> waits = <Duration>[];
    for (String part in value.split(',')) {
      int? seconds = int.tryParse(part.trim());
      if (seconds == null || seconds < 0) {
        throw FormatException(
          '--backoff needs seconds of 0 or more, eg. 1,5,10',
        );
      }
      waits.add(Duration(seconds: seconds));
    }
    return waits;
  }

  static const List<String> _flags = <String>[
    'queue',
    'concurrency',
    'once',
    'tries',
    'timeout',
    'sleep',
    'stop-when-empty',
    'timeout-grace',
    'backoff',
    'max-jobs',
    'max-time',
    'memory',
    'hours',
    'connection',
    'unfinished',
  ];

  static String _trim(String value) => value.trim();

  /// a flag in seconds, or null when the flag is not given
  static Duration? _seconds(Map<String, String> flags, String key) {
    int? seconds = _int(flags, key);
    return seconds == null ? null : Duration(seconds: seconds);
  }

  /// a whole number of 0 or more, or null when the flag is not given
  static int? _int(Map<String, String> flags, String key) {
    String? value = flags[key];
    if (value == null) return null;
    int? number = int.tryParse(value);
    int min =
        const <String>[
          'concurrency',
          'tries',
          'max-jobs',
          'memory',
          'max-time',
        ].contains(key)
        ? 1
        : 0;
    if (number == null || number < min) {
      throw FormatException('--$key needs a number of $min or more');
    }
    return number;
  }
}
