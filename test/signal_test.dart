import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:dox_orm/dox_orm.dart';
import 'package:dox_queue/dox_queue.dart';
import 'package:test/test.dart';

import 'fixtures/jobs.g.dart';
import 'fixtures/test_jobs.dart';
import 'support.dart';

void main() {
  test('SIGTERM stops the worker after the running job', () async {
    Directory dir = Directory.systemTemp.createTempSync('dox_queue_');
    String path = '${dir.path}/queue.sqlite';
    setUpTestApp();
    SqlQueryBuilder.initializeWithDatabaseConfig(
      DatabaseConfig(
        connection: 'file',
        connections: <String, ConnectionConfig>{
          'file': ConnectionConfig(
            driver: Driver.sqlite,
            database: path,
            debug: false,
          ),
        },
      ),
    );
    await QueueTables.up(Schema());
    Queue.configure(
      QueueConfig(driver: 'database', jobs: jobRegistry, connection: 'file'),
    );
    await SlowJob(key: 'sig', millis: 1500).dispatch();
    await SendWelcomeEmail(userId: 1).dispatch();

    Process process = await Process.start(Platform.resolvedExecutable, <String>[
      'run',
      'test/fixtures/worker_main.dart',
      path,
      'work',
      '--queue=slow,default',
    ]);
    StringBuffer out = StringBuffer();
    Completer<void> started = Completer<void>();
    process.stdout.transform(utf8.decoder).listen((String text) {
      out.write(text);
      if (text.contains('running SlowJob') && !started.isCompleted) {
        started.complete();
      }
    });
    process.stderr.transform(utf8.decoder).listen(out.write);
    await started.future.timeout(const Duration(seconds: 60));
    // SIGTERM
    process.kill();
    int code = await process.exitCode.timeout(const Duration(seconds: 10));

    String log = out.toString();
    expect(code, 0, reason: log);
    expect(log, contains('done SlowJob'));
    expect(log, contains('worker stopped'));
    // it took no new job after the signal
    expect(log, isNot(contains('running SendWelcomeEmail')));
    ReservedJob? left = await Queue.driver.pop('default');
    expect(left?.payload.job, 'SendWelcomeEmail');
    expect(await Queue.driver.pop('slow'), isNull);
    await Queue.close();
    await SqlQueryBuilder.close();
    dir.deleteSync(recursive: true);
  }, timeout: const Timeout(Duration(minutes: 2)));
}
