import 'package:dox_core/dox_core.dart' show Cache;

import 'queue_driver.dart';
import 'queue_log.dart';

/// Internal, not exported: free the unique lock [name] of the job [id].
/// Only the lock of this job is freed, not the lock of a newer job that
/// took it after this one expired. An error is logged, never thrown.
Future<void> releaseUniqueLock(String name, String id) async {
  try {
    await Cache().lock(name, null, id).release();
  } catch (error, stack) {
    QueueLog.danger(
      'Queue: can not free the unique lock $name of $id: $error',
      '$stack',
    );
  }
}

/// For queue drivers: free the unique lock of the saved job [text] (a
/// [JobPayload.encode] text), if it has one. `clear` of the drivers calls
/// it for each job it deletes.
Future<void> freeUniqueLockOf(String text) async {
  // most jobs are not unique: skip the JSON decode for them
  if (!text.contains('"unique":')) return;
  JobPayload payload;
  try {
    payload = JobPayload.decode(text);
  } catch (error) {
    // one broken job must not stop the clear of the others
    QueueLog.warn('Queue: can not read a cleared job: $error');
    return;
  }
  String? name = payload.unique;
  if (name != null) await releaseUniqueLock(name, payload.id);
}
