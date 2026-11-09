import 'dart:convert';

import 'package:ioredis/ioredis.dart';

import '../queue.dart';
import '../queue_driver.dart';

/// Keeps jobs in Redis. Each queue has three keys:
/// - `<prefix>queue:<name>`: a list of jobs that are ready,
/// - `<prefix>queue:<name>:delayed`: a sorted set of jobs for later,
///   scored by the time they are due,
/// - `<prefix>queue:<name>:reserved`: a sorted set of jobs that a worker
///   runs now, scored by the time the reservation ends.
///
/// A worker takes a job with one Lua script, so two workers never get
/// the same job. When a worker crashes, its job stays in the reserved
/// set. After [retryAfter] the next pop moves it back to the list.
/// Failed jobs are in the hash `<prefix>failed`.
class RedisQueueDriver extends QueueDriver {
  RedisQueueDriver(
    this.redis, {
    this.prefix = 'dox_queue:',
    this.retryAfter = const Duration(seconds: 90),
  });

  final Redis redis;
  final String prefix;
  final Duration retryAfter;

  /// Moves due delayed jobs and expired reservations to the list, then
  /// takes the first job: adds 1 to its attempts and reserves it.
  /// The data is a string inside the payload, so cjson keeps it as it is.
  static const String _popScript = '''
local function migrate(from, to, now)
  local due = redis.call('ZRANGEBYSCORE', from, '-inf', now, 'LIMIT', 0, 100)
  for i = 1, #due do
    redis.call('ZREM', from, due[i])
    redis.call('RPUSH', to, due[i])
  end
end
migrate(KEYS[2], KEYS[1], ARGV[1])
migrate(KEYS[3], KEYS[1], ARGV[1])
local job = redis.call('LPOP', KEYS[1])
if not job then
  return false
end
local decoded = cjson.decode(job)
decoded['attempts'] = (decoded['attempts'] or 0) + 1
local reserved = cjson.encode(decoded)
redis.call('ZADD', KEYS[3], ARGV[2], reserved)
return reserved''';

  /// Moves a reserved job to the delayed set, as the new text ARGV[3]
  /// (it can have a new exception count).
  static const String _releaseScript = '''
redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('ZADD', KEYS[2], ARGV[2], ARGV[3])
return 1''';

  String _list(String queue) => '${prefix}queue:$queue';

  String _delayed(String queue) => '${prefix}queue:$queue:delayed';

  String _reserved(String queue) => '${prefix}queue:$queue:reserved';

  String get _failed => '${prefix}failed';

  @override
  Future<void> push(
    JobPayload payload, {
    Duration delay = Duration.zero,
  }) async {
    String text = payload.encode();
    if (delay > Duration.zero) {
      int at = QueueDriver.now() + delay.inMilliseconds;
      await redis.zadd(_delayed(payload.queue), <String, num>{text: at});
      return;
    }
    await redis.rpush(_list(payload.queue), text);
  }

  @override
  Future<ReservedJob?> pop(String queue) async {
    int now = QueueDriver.now();
    Object? reply = await redis.eval(_popScript, 3, <Object>[
      _list(queue),
      _delayed(queue),
      _reserved(queue),
      now,
      now + retryAfter.inMilliseconds,
    ]);
    if (reply == null) return null;
    String text = reply.toString();
    JobPayload payload = JobPayload.decode(text);
    return ReservedJob(payload, attempts: payload.attempts, ref: text);
  }

  @override
  Future<void> delete(ReservedJob job) async {
    await redis.zrem(_reserved(job.payload.queue), job.ref);
  }

  @override
  Future<void> release(ReservedJob job, Duration delay) async {
    String queue = job.payload.queue;
    await redis.eval(_releaseScript, 2, <Object>[
      _reserved(queue),
      _delayed(queue),
      job.ref,
      QueueDriver.now() + delay.inMilliseconds,
      job.payload.encode(),
    ]);
  }

  @override
  Future<void> fail(ReservedJob job, String error) async {
    JobPayload payload = job.payload;
    // save first: a crash in between gives a copy, not a lost job
    await redis.hset(_failed, <String, Object>{
      payload.id: jsonEncode(<String, Object?>{
        'queue': payload.queue,
        'payload': payload.withAttempts(job.attempts).encode(),
        'error': error,
        'failedAt': QueueDriver.now(),
      }),
    });
    await redis.zrem(_reserved(payload.queue), job.ref);
  }

  @override
  Future<List<FailedJob>> failed() async {
    Map<String, String> all = await redis.hgetall(_failed);
    List<FailedJob> jobs = <FailedJob>[
      for (MapEntry<String, String> entry in all.entries)
        _failedJob(entry.key, entry.value),
    ];
    jobs.sort(_oldestFirst);
    return jobs;
  }

  @override
  Future<bool> retry(String id) async {
    String? text = await redis.hget(_failed, id);
    if (text == null) return false;
    FailedJob job = _failedJob(id, text);
    await push(Queue.retriedPayload(job.payload));
    await redis.hdel(_failed, id);
    return true;
  }

  @override
  Future<bool> forget(String id) async {
    return await redis.hdel(_failed, id) > 0;
  }

  @override
  Future<void> flush() async {
    await redis.del(_failed);
  }

  /// counts the jobs of the three keys of a queue, then deletes them
  static const String _clearScript = '''
local total = redis.call('LLEN', KEYS[1])
  + redis.call('ZCARD', KEYS[2])
  + redis.call('ZCARD', KEYS[3])
redis.call('DEL', KEYS[1], KEYS[2], KEYS[3])
return total''';

  @override
  Future<int> clear(String queue) async {
    Object? total = await redis.eval(_clearScript, 3, <Object>[
      _list(queue),
      _delayed(queue),
      _reserved(queue),
    ]);
    return int.parse('$total');
  }

  @override
  Future<int> pruneFailed(DateTime before) async {
    List<String> ids = <String>[
      for (FailedJob job in await failed())
        if (job.failedAt.isBefore(before)) job.id,
    ];
    if (ids.isNotEmpty) await redis.hdel(_failed, ids);
    return ids.length;
  }

  /// close the connection and the pool, with no reconnect
  @override
  Future<void> close() async {
    redis.pool.destroyAll();
    await redis.disconnect();
  }

  static FailedJob _failedJob(String id, String text) {
    Map<String, Object?> map = jsonDecode(text) as Map<String, Object?>;
    return FailedJob(
      id: id,
      queue: map['queue'] as String,
      payload: map['payload'] as String,
      error: map['error'] as String,
      failedAt: DateTime.fromMillisecondsSinceEpoch(
        (map['failedAt'] as num).toInt(),
      ),
    );
  }

  static int _oldestFirst(FailedJob a, FailedJob b) {
    return a.failedAt.compareTo(b.failedAt);
  }
}
