import 'dart:io';

import 'package:dox_core/dox_core.dart' show Env;
import 'package:dox_queue/dox_queue.dart';
import 'package:test/test.dart';

void main() {
  group('QueueConfig.redisFromEnv', () {
    test('no keys: the old default, localhost:6379', () {
      RedisOptions options = QueueConfig.redisFromEnv(<String, String>{});
      expect(options.host, '127.0.0.1');
      expect(options.port, 6379);
      expect(options.password, isNull);
      expect(options.username, isNull);
      expect(options.secure, isFalse);
      expect(options.db, 0);
    });

    test('reads REDIS_HOST, REDIS_PORT, REDIS_PASSWORD and REDIS_TLS', () {
      RedisOptions options = QueueConfig.redisFromEnv(<String, String>{
        'REDIS_HOST': 'cache.local',
        'REDIS_PORT': '6380',
        'REDIS_USERNAME': 'app',
        'REDIS_PASSWORD': 'secret',
        'REDIS_TLS': 'true',
      });
      expect(options.host, 'cache.local');
      expect(options.port, 6380);
      expect(options.username, 'app');
      expect(options.password, 'secret');
      expect(options.secure, isTrue);
    });

    test('REDIS_URL wins over the other keys', () {
      RedisOptions options = QueueConfig.redisFromEnv(<String, String>{
        'REDIS_URL': 'rediss://app:p%40ss@redis.example.com:6390/2',
        'REDIS_HOST': 'ignored',
        'REDIS_PASSWORD': 'ignored',
      });
      expect(options.host, 'redis.example.com');
      expect(options.port, 6390);
      expect(options.username, 'app');
      expect(options.password, 'p@ss');
      expect(options.db, 2);
      expect(options.secure, isTrue);
    });

    test('REDIS_URL with only a password', () {
      RedisOptions options = QueueConfig.redisFromEnv(<String, String>{
        'REDIS_URL': 'redis://:secret@10.0.0.5',
      });
      expect(options.host, '10.0.0.5');
      expect(options.port, 6379);
      expect(options.username, isNull);
      expect(options.password, 'secret');
      expect(options.secure, isFalse);
    });

    test('bad values throw a clear error', () {
      expect(
        () => QueueConfig.redisFromEnv(<String, String>{'REDIS_PORT': 'x'}),
        throwsArgumentError,
      );
      expect(
        () => QueueConfig.redisFromEnv(<String, String>{
          'REDIS_URL': 'http://host',
        }),
        throwsArgumentError,
      );
    });
  });

  group('REDIS_URL errors never hold the password', () {
    const String pass = 'p@ss/word';
    void expectMasked(String url) {
      Object? error;
      try {
        QueueConfig.redisFromEnv(<String, String>{'REDIS_URL': url});
      } catch (e) {
        error = e;
      }
      expect(error, isNotNull, reason: url);
      String text = '$error'.toLowerCase();
      expect(text, isNot(contains(pass.toLowerCase())), reason: text);
      expect(text, isNot(contains('p%40ss')), reason: text);
      expect(text, isNot(contains('secret')), reason: text);
      expect(text, contains('redis_url'));
    }

    test('bad port', () => expectMasked('redis://app:p%40ss%2Fword@host:99x'));
    test('wrong scheme', () => expectMasked('http://app:p%40ss%2Fword@host'));
    test('password with /', () => expectMasked('redis://app:p@ss/word@host:x'));
    test('password with @', () => expectMasked('redis://app:p@ss@host:abc/1'));
    test('db part that is not a number', () {
      expectMasked('redis://:12/p@ss/word@host');
    });
    test('scheme with a single colon', () {
      expectMasked('rediz:user:SECRET@host');
    });
    test('password in the query string', () {
      expectMasked('http://host?password=SECRET');
    });
    test('no scheme, token before the colon', () {
      expectMasked('tok3nSecret:@host/0');
      expectMasked('tok3nSecret:pw@host');
      for (String url in <String>[
        'tok3nSecret:@host/0',
        'tok3nSecret:pw@host',
      ]) {
        Object? error;
        try {
          QueueConfig.redisFromEnv(<String, String>{'REDIS_URL': url});
        } catch (e) {
          error = e;
        }
        expect('$error'.toLowerCase(), isNot(contains('tok3nsecret')));
      }
    });
    test('password with #', () => expectMasked('redis://app:p#ss@host:abc'));
  });

  group('createDriver', () {
    bool realEnvHasUrl = Platform.environment.containsKey('REDIS_URL');

    tearDown(() {
      Env().env.remove('REDIS_URL');
      Env().env.remove('REDIS_HOST');
    });

    test('redis null keeps localhost:6379 and does not read the env', () {
      Env().env['REDIS_URL'] = 'http://not-redis';
      Env().env['REDIS_HOST'] = '10.0.0.9';
      RedisQueueDriver driver =
          QueueConfig(driver: 'redis').createDriver() as RedisQueueDriver;
      expect(driver.redis.option.host, '127.0.0.1');
      expect(driver.redis.option.port, 6379);
    });

    test('redisFromEnv() reads the env, real env and .env', () {
      Env().env['REDIS_URL'] = 'redis://:secret@10.0.0.9:6391';
      RedisQueueDriver driver =
          QueueConfig(
                driver: 'redis',
                redis: QueueConfig.redisFromEnv(),
              ).createDriver()
              as RedisQueueDriver;
      expect(driver.redis.option.host, '10.0.0.9');
      expect(driver.redis.option.port, 6391);
      expect(driver.redis.option.password, 'secret');
    }, skip: realEnvHasUrl ? 'REDIS_URL is set in the real env' : false);

    test('explicit redis options win over the env', () {
      Env().env['REDIS_URL'] = 'redis://:secret@10.0.0.9:6391';
      RedisQueueDriver driver =
          QueueConfig(
                driver: 'redis',
                redis: RedisOptions(host: 'own.host', port: 7000),
              ).createDriver()
              as RedisQueueDriver;
      expect(driver.redis.option.host, 'own.host');
      expect(driver.redis.option.port, 7000);
      expect(driver.redis.option.password, isNull);
    });
  });

  test('inProcess, and the deprecated workInProcess', () {
    expect(QueueConfig().inProcess, isFalse);
    expect(QueueConfig(inProcess: true).inProcess, isTrue);
    // ignore: deprecated_member_use_from_same_package
    QueueConfig old = QueueConfig(workInProcess: true);
    expect(old.inProcess, isTrue);
    // ignore: deprecated_member_use_from_same_package
    expect(old.workInProcess, isTrue);
    // both: inProcess wins
    expect(
      // ignore: deprecated_member_use_from_same_package
      QueueConfig(inProcess: false, workInProcess: true).inProcess,
      isFalse,
    );
  });
}
