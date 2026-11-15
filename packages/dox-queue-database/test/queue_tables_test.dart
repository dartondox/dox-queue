import 'package:dox_orm/dox_orm.dart';
import 'package:dox_queue_database/dox_queue_database.dart';
import 'package:test/test.dart';

import 'support.dart';

/// the indexes of the `jobs` table, each as its columns in order, like
/// `queue,reserved_at,available_at`. The primary key is left out.
Future<Set<String>> _indexes(DBDriver db) async {
  switch (db.getName()) {
    case Driver.sqlite:
      Set<String> out = <String>{};
      for (Map<String, dynamic> index in await db.query(
        "SELECT name FROM pragma_index_list('jobs') WHERE origin = 'c'",
      )) {
        List<Map<String, dynamic>> columns = await db.query(
          "SELECT name FROM pragma_index_info('${index['name']}') "
          'ORDER BY seqno',
        );
        out.add(columns.map(_name).join(','));
      }
      return out;
    case Driver.postgres:
      List<Map<String, dynamic>> rows = await db.query(
        'SELECT a.attname AS name, i.indexrelid AS id, k.n AS n '
        'FROM pg_index i '
        'CROSS JOIN LATERAL unnest(i.indkey::int2[]) WITH ORDINALITY AS k(col, n) '
        'JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.col '
        "WHERE i.indrelid = 'jobs'::regclass AND NOT i.indisprimary "
        'ORDER BY i.indexrelid, k.n',
      );
      return _group(rows, 'id');
    case Driver.mysql:
      List<Map<String, dynamic>> rows = await db.query(
        'SELECT index_name AS id, column_name AS name '
        'FROM information_schema.statistics WHERE table_schema = DATABASE() '
        "AND table_name = 'jobs' AND index_name <> 'PRIMARY' "
        'ORDER BY index_name, seq_in_index',
      );
      return _group(rows, 'id');
  }
}

String _name(Map<String, dynamic> row) => '${row['name']}';

/// the column names of [rows], joined per index [key]
Set<String> _group(List<Map<String, dynamic>> rows, String key) {
  Map<String, List<String>> byIndex = <String, List<String>>{};
  for (Map<String, dynamic> row in rows) {
    (byIndex['${row[key]}'] ??= <String>[]).add(_name(row));
  }
  return <String>{for (List<String> names in byIndex.values) names.join(',')};
}

/// the `jobs` table as it was before the pop index
void _oldJobs(Table table) {
  table.id();
  table.string('queue').index();
  table.longText('payload');
  table.integer('attempts').defaultTo(0);
  table.bigInteger('reserved_at').nullable();
  table.bigInteger('available_at');
  table.bigInteger('created_at');
}

void main() {
  setUpAll(setUpTestApp);

  for (TestStore store in testStores) {
    if (store.isRedis) continue;
    group('${store.name} jobs table', () {
      late Schema schema;
      bool mysql = store.name == 'mysql';

      setUp(() async {
        schema = Schema(SqlQueryBuilder().getDBDriver(store.name));
        await QueueTables.down(schema);
      });

      tearDown(() async {
        // leave the tables of up(), as the other tests expect them
        await QueueTables.down(schema);
        await QueueTables.up(schema);
      });

      test('a new table has the pop index', () async {
        await QueueTables.up(schema);
        expect(
          await _indexes(schema.driver),
          mysql
              ? <String>{'queue'}
              : <String>{QueueTables.popIndexColumns.join(',')},
        );
      });

      test('popIndex adds it to an old table', () async {
        await schema.create('jobs', _oldJobs);
        await QueueTables.popIndex(schema);
        expect(
          await _indexes(schema.driver),
          mysql
              ? <String>{'queue'}
              : <String>{'queue', QueueTables.popIndexColumns.join(',')},
        );
      });

      test('popIndex is safe to call again', () async {
        await QueueTables.up(schema);
        await QueueTables.popIndex(schema);
        await QueueTables.popIndex(schema);
        expect(
          await _indexes(schema.driver),
          mysql
              ? <String>{'queue'}
              : <String>{QueueTables.popIndexColumns.join(',')},
        );
        await schema.dropIfExists('jobs');
        await schema.create('jobs', _oldJobs);
        await QueueTables.popIndex(schema);
        await QueueTables.popIndex(schema);
        expect(
          await _indexes(schema.driver),
          mysql
              ? <String>{'queue'}
              : <String>{'queue', QueueTables.popIndexColumns.join(',')},
        );
      });
    }, skip: store.skip);
  }
}
