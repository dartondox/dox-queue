import 'dart:io';

import 'package:dox_annotation/command.dart';
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

  test('does not add a second one to an app from the template', () async {
    /// the template has a 9-digit time in this file name
    File('${folder.path}/2026_10_25_000000000_create_queue_tables.dart')
        .writeAsStringSync('// template');
    await QueueTableCommand(migrations: MigrationFile(folder: folder))
        .run(<String>[]);
    expect(folder.listSync(), hasLength(1));
    expect(exitCode, 1);
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
    expect(
      files.first.readAsStringSync(),
      contains('QueueTables.upBatches(schema)'),
    );
    expect(exitCode, 0);

    await command.run(<String>[]);
    expect(folder.listSync(), hasLength(1));
    expect(exitCode, 1);
  });
}
