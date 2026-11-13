import 'dart:io';

import 'package:dox_annotation/install.dart';
import 'package:dox_queue/commands.dart';
import 'package:test/test.dart';

/// records the `dart` commands, runs nothing
class FakeRunner extends ProcessRunner {
  final List<String> calls = <String>[];

  @override
  Future<int> run(String executable, List<String> args, String folder) async {
    calls.add(args.join(' '));
    return 0;
  }
}

void main() {
  late Directory root;
  late FakeRunner runner;

  setUp(() {
    root = Directory.systemTemp.createTempSync('dox_queue_install_');
    File('${root.path}/pubspec.yaml').writeAsStringSync(
      'name: my_app\n\ndependencies:\n  dox_core: ^3.1.0\n'
      '  dox_migration: ^3.1.0\n  dox_builder: ^3.1.0\n',
    );
    File('${root.path}/.env').writeAsStringSync('APP_KEY=x\n');
    runner = FakeRunner();
    exitCode = 0;
  });

  tearDown(() {
    root.deleteSync(recursive: true);
  });

  String read(String path) => File('${root.path}/$path').readAsStringSync();

  bool exists(String path) => File('${root.path}/$path').existsSync();

  Future<void> install(List<String> args) {
    return QueueInstallCommand(root: root, runner: runner).run(args);
  }

  /// every file of the app and its text
  Map<String, String> snapshot() {
    Map<String, String> files = <String, String>{};
    for (FileSystemEntity f in root.listSync(recursive: true)) {
      if (f is File) files[f.path] = f.readAsStringSync();
    }
    return files;
  }

  String migrationPath() {
    return Directory('${root.path}/db/migration')
        .listSync()
        .single
        .path
        .split('/')
        .last;
  }

  test('sets up the database driver by default', () async {
    await install(<String>[]);
    expect(exitCode, 0);
    expect(runner.calls, contains('pub add dox_queue_database'));
    expect(runner.calls, contains('pub add --dev build_runner'));
    expect(runner.calls, isNot(contains('pub add --dev dox_builder')));
    expect(runner.calls.last, 'run build_runner build');
    expect(
      read('lib/config/queue.dart'),
      allOf(
        contains("import 'package:my_app/generated/jobs.g.dart';"),
        contains("driver: 'database'"),
        isNot(contains('redis:')),
      ),
    );
    expect(
      read('lib/providers/queue_provider.dart'),
      contains('AppQueueProvider extends DatabaseQueueProvider'),
    );
    expect(read('bin/queue.dart'), contains('package:my_app/config/app.dart'));
    expect(exists('lib/app/jobs/send_welcome_email.job.dart'), isTrue);
    expect(migrationPath(), endsWith('_create_queue_tables.dart'));
    expect(read('.env'), isNot(contains('REDIS')));
  });

  test('redis adds its keys and the plain provider', () async {
    await install(<String>['--driver=redis']);
    expect(runner.calls, isNot(contains('pub add dox_queue_database')));
    expect(
      read('lib/config/queue.dart'),
      contains('QueueConfig.redisFromEnv()'),
    );
    expect(
      read('lib/providers/queue_provider.dart'),
      contains('AppQueueProvider extends QueueProvider'),
    );
    expect(read('.env'), contains('REDIS_HOST=127.0.0.1\nREDIS_PORT=6379\n'));
    expect(Directory('${root.path}/db').existsSync(), isFalse);
  });

  test('sync uses the plain provider and no keys', () async {
    await install(<String>['--driver', 'sync']);
    expect(read('lib/config/queue.dart'), contains("driver: 'sync'"));
    expect(read('.env'), 'APP_KEY=x\n');
    expect(Directory('${root.path}/db').existsSync(), isFalse);
  });

  test(
    'the database driver without dox_migration skips the migration',
    () async {
      File(
        '${root.path}/pubspec.yaml',
      ).writeAsStringSync('name: my_app\ndependencies:\n  dox_core: ^3.1.0\n');
      await install(<String>[]);
      expect(exitCode, 0);
      expect(Directory('${root.path}/db').existsSync(), isFalse);
      expect(exists('lib/config/queue.dart'), isTrue);
    },
  );

  test('a second run changes nothing', () async {
    await install(<String>['--driver=redis']);
    File('${root.path}/lib/config/queue.dart').writeAsStringSync('mine');
    Map<String, String> before = snapshot();
    await install(<String>['--driver=redis']);
    expect(snapshot(), before);
  });

  test('a second database run adds no second migration', () async {
    await install(<String>[]);
    Map<String, String> before = snapshot();
    await install(<String>[]);
    expect(snapshot(), before);
  });

  test('a bad option stops with exit code 1 and writes nothing', () async {
    for (List<String> args in <List<String>>[
      <String>['--driver=oracle'],
      <String>['--drive=redis'],
      <String>['--driver'],
      <String>['--driver='],
      <String>['redis'],
    ]) {
      Map<String, String> before = snapshot();
      await install(args);
      expect(exitCode, 1, reason: '$args');
      expect(runner.calls, isEmpty, reason: '$args');
      expect(snapshot(), before, reason: '$args');
      exitCode = 0;
    }
  });

  test('existing .env keys and comments stay the same', () async {
    String env = '# mine\nREDIS_HOST=10.0.0.5 # keep\nAPP_KEY=x\n';
    File('${root.path}/.env').writeAsStringSync(env);
    await install(<String>['--driver=redis']);
    expect(read('.env'), startsWith(env));
    expect(read('.env'), isNot(contains('REDIS_HOST=127')));
  });

  test(
    'a second run with another driver keeps the config and its keys',
    () async {
      await install(<String>['--driver=sync']);
      Map<String, String> before = snapshot();
      runner.calls.clear();
      await install(<String>['--driver=redis']);
      expect(snapshot(), before);
      expect(runner.calls, isNot(contains('pub add dox_queue_database')));
      expect(read('.env'), isNot(contains('REDIS')));
      expect(read('lib/config/queue.dart'), contains("driver: 'sync'"));
    },
  );

  test('the config has the line breaks of the template', () async {
    await install(<String>['--driver=sync']);
    expect(
      read('lib/config/queue.dart'),
      contains(
        "\nQueueConfig queueConfig = QueueConfig(driver: 'sync', jobs: jobRegistry);\n",
      ),
    );
  });
}
