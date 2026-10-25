import 'dart:io';

import 'package:build/build.dart';
import 'package:build_test/build_test.dart';
import 'package:dart_style/dart_style.dart';
import 'package:dox_queue/builder.dart';
import 'package:test/test.dart';

/// a small copy of dox_queue: the builder finds Job and DoxJob by name,
/// in a dox_queue library
const String _queue = '''
class Job {
  const Job({this.name});
  final String? name;
}
abstract class DoxJob {
  Future<void> handle();
  int? get tries => null;
  List<Duration> get backoff => const <Duration>[];
  Duration? get timeout => null;
  String get queue => 'default';
  void failed(Object error, StackTrace stack) {}
}
''';

const String _golden = 'test/fixtures/jobs.g.dart';

/// the generated file, or the build errors
Future<String> _generate(Map<String, String> app) async {
  Map<String, String> sources = <String, String>{
    'dox_queue|lib/dox_queue.dart': _queue,
    ...app,
  };
  TestBuilderResult result = await testBuilder(
    buildJobRegistry(BuilderOptions.empty),
    sources,
    rootPackage: 'a',
  );
  if (!result.succeeded) return 'ERROR: ${result.errors.join('\n')}';
  String output = '';
  await testBuilder(
    buildJobRegistry(BuilderOptions.empty),
    sources,
    rootPackage: 'a',
    outputs: <String, Object>{
      // the test copy of dox_queue gets one too
      'dox_queue|lib/jobs.g.dart': anything,
      'a|lib/jobs.g.dart': decodedMatches(
        predicate<String>((String s) {
          output = s;
          return true;
        }),
      ),
    },
  );
  return output;
}

Future<String> _error(String source) async {
  String result = await _generate(<String, String>{'a|lib/jobs.dart': source});
  expect(result, startsWith('ERROR'));
  return result;
}

const String _header = '''
import 'package:dox_queue/dox_queue.dart';
enum Color { red }
''';

void main() {
  test('writes the registry of test/fixtures/test_jobs.dart', () async {
    String source = File('test/fixtures/test_jobs.dart').readAsStringSync();
    String output = await _generate(<String, String>{
      'a|lib/test_jobs.dart': source,
    });
    expect(output, isNot(startsWith('ERROR')), reason: output);
    // the checked-in copy imports the fixture by a relative path
    output = DartFormatter(
      languageVersion: DartFormatter.latestLanguageVersion,
    ).format(output.replaceAll('package:a/test_jobs.dart', 'test_jobs.dart'));
    File golden = File(_golden);
    if (Platform.environment['UPDATE_GOLDEN'] == '1') {
      golden.writeAsStringSync(output);
    }
    expect(output, golden.readAsStringSync());
    expect(output, contains("JobCodec('all-types', i1.AllTypesJob"));
    expect(output, contains("JobCodec('SendWelcomeEmail'"));
  });

  test('writes an empty registry when there is no job', () async {
    String output = await _generate(<String, String>{
      'a|lib/user.dart': 'class User {}',
    });
    expect(output, contains('const JobRegistry jobRegistry'));
    expect(output, isNot(contains('JobCodec(')));
  });

  test('a job class must extend DoxJob', () async {
    String error = await _error('''
$_header
@Job()
class A { A(); }
''');
    expect(error, contains('must extend DoxJob'));
  });

  test('a parameter type must be a JSON type', () async {
    String error = await _error('''
$_header
class User {}
@Job()
class A extends DoxJob {
  A(this.user);
  final User user;
  @override
  Future<void> handle() async {}
}
''');
    expect(error, contains('parameter "user"'));
    expect(error, contains('User is not a JSON type'));
  });

  test('map keys must be String', () async {
    String error = await _error('''
$_header
@Job()
class A extends DoxJob {
  A(this.byId);
  final Map<int, String> byId;
  @override
  Future<void> handle() async {}
}
''');
    expect(error, contains('map keys must be String'));
  });

  test('a parameter needs a field of the same name', () async {
    String error = await _error('''
$_header
@Job()
class A extends DoxJob {
  A(int id) : userId = id;
  final int userId;
  @override
  Future<void> handle() async {}
}
''');
    expect(error, contains('needs a field or getter "id"'));
  });

  test('a private parameter can not be saved', () async {
    String error = await _error('''
$_header
@Job()
class A extends DoxJob {
  A(this._id);
  final int _id;
  @override
  Future<void> handle() async { print(_id); }
}
''');
    expect(error, contains('private parameter'));
  });

  test('two jobs can not have one name', () async {
    String error = await _error('''
$_header
@Job(name: 'same')
class A extends DoxJob {
  @override
  Future<void> handle() async {}
}
@Job(name: 'same')
class B extends DoxJob {
  @override
  Future<void> handle() async {}
}
''');
    expect(error, contains('Two jobs are named "same"'));
  });
}
