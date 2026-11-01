import 'dart:io';

import 'package:dox_core/command.dart';
import 'package:dox_queue/commands.dart';
import 'package:test/test.dart';

void main() {
  late Directory folder;

  setUp(() {
    folder = Directory.systemTemp.createTempSync('dox_queue_table_');
  });

  tearDown(() {
    exitCode = 0;
    folder.deleteSync(recursive: true);
  });

  test('commands has queue:table', () {
    expect(commands.map((Command c) => c.name), contains('queue:table'));
  });

  test('writes the migration once', () async {
    QueueTableCommand command = QueueTableCommand(
      migrations: MigrationFile(folder: folder),
    );
    await command.run(<String>[]);
    List<File> files = folder.listSync().whereType<File>().toList();
    expect(files, hasLength(1));
    expect(files.first.path, endsWith('_create_queue_tables.dart'));
    expect(files.first.readAsStringSync(), contains('QueueTables.up(schema)'));
    expect(exitCode, 0);

    await command.run(<String>[]);
    expect(folder.listSync(), hasLength(1));
    expect(exitCode, 1);
  });
}
