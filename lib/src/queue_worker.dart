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
///   `--sleep=SECONDS`, `--stop-when-empty`.
/// - `failed`: list the failed jobs.
/// - `retry <id|all>`: put failed jobs back on their queue.
/// - `forget <id>`: delete one failed job.
/// - `flush`: delete all failed jobs.
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

  static const String usage = '''
Usage: dart run bin/queue.dart <command> [flags]

Commands:
  work              run jobs (default)
  failed            list the failed jobs
  retry <id|all>    put failed jobs back on their queue
  forget <id>       delete one failed job
  flush             delete all failed jobs

Flags of work:
  --queue=a,b         the queues to work, first one first (default: default)
  --concurrency=N     jobs at once in this process (default: 1)
  --once              run one job, then stop
  --tries=N           tries of a job that does not set tries (default: 1)
  --timeout=SECONDS   max run time of a job (default: 60)
  --sleep=SECONDS     the wait when the queues are empty (default: 3)
  --stop-when-empty   stop when the queues are empty''';

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
        return _work(command.options);
      case 'failed':
        _printFailed(await Queue.driver.failed());
        return 0;
      case 'retry':
        return _retry(command.argument);
      case 'forget':
        bool found = await Queue.driver.forget(command.argument);
        stdout.writeln(found ? 'Deleted failed job.' : 'No failed job found.');
        return found ? 0 : 1;
      case 'flush':
        await Queue.driver.flush();
        stdout.writeln('Deleted all failed jobs.');
        return 0;
    }
    return 64;
  }

  Future<int> _work(WorkerOptions options) async {
    if (Queue.config.driver == 'sync') {
      stdout.writeln(
        'The sync driver runs jobs on dispatch. No worker needed.',
      );
      return 0;
    }
    Worker worker = Worker(
      driver: Queue.driver,
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

    signals.add(ProcessSignal.sigint.watch().listen(onSignal));
    // Windows can not watch SIGTERM
    if (!Platform.isWindows) {
      signals.add(ProcessSignal.sigterm.watch().listen(onSignal));
    }
    Logger.info(
      'Queue: worker started on ${options.queues.join(', ')} '
      '(${Queue.config.driver}, concurrency ${options.concurrency})',
    );
    await worker.run();
    for (StreamSubscription<ProcessSignal> s in signals) {
      await s.cancel();
    }
    Logger.info('Queue: worker stopped');
    return 0;
  }

  Future<int> _retry(String id) async {
    QueueDriver driver = Queue.driver;
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
  QueueCommand(this.name, this.argument, this.options);

  /// `work`, `failed`, `retry`, `forget` or `flush`
  final String name;

  /// the id of `retry` and `forget`
  final String argument;
  final WorkerOptions options;

  static const List<String> _commands = <String>[
    'work',
    'failed',
    'retry',
    'forget',
    'flush',
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
      ),
    );
  }

  static const List<String> _flags = <String>[
    'queue',
    'concurrency',
    'once',
    'tries',
    'timeout',
    'sleep',
    'stop-when-empty',
  ];

  static String _trim(String value) => value.trim();

  /// a whole number of 0 or more, or null when the flag is not given
  static int? _int(Map<String, String> flags, String key) {
    String? value = flags[key];
    if (value == null) return null;
    int? number = int.tryParse(value);
    int min = key == 'concurrency' || key == 'tries' ? 1 : 0;
    if (number == null || number < min) {
      throw FormatException('--$key needs a number of $min or more');
    }
    return number;
  }
}
