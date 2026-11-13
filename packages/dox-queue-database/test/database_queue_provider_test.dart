import 'package:dox_queue/dox_queue.dart';
import 'package:dox_queue_database/dox_queue_database.dart';
import 'package:test/test.dart';

// No support.dart here: it calls QueueDatabase.register(), and this test
// needs an isolate where nothing is registered yet.
void main() {
  test('DatabaseQueueProvider registers the database driver', () async {
    QueueConfig config = QueueConfig(driver: 'database');
    expect(Queue.factoryOf('database'), isNull);
    expect(config.createDriver, throwsArgumentError);

    await DatabaseQueueProvider(config).setup();

    expect(Queue.factoryOf('database'), isA<DatabaseQueueDriverFactory>());
    expect(Queue.batches, isA<DatabaseBatchRepository>());
    expect(Queue.config, same(config));
  });
}
