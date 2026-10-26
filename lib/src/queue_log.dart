import 'dart:io';

import 'package:dox_core/dox_core.dart';
import 'package:dox_core/utils/logger.dart';

/// Logs of the queue worker.
///
/// It uses the Dox `Logger` when `Dox().initialize()` ran. Without it,
/// `Logger` crashes, so the line goes to stdout, or stderr for errors.
class QueueLog {
  static void info(String message) => _log('info', message);

  static void success(String message) => _log('success', message);

  static void warn(String message) => _log('warn', message);

  static void danger(String message, [String? data]) {
    _log('danger', message, data);
  }

  static void _log(String level, String message, [String? data]) {
    if (_doxReady()) {
      Logger.log(level, message, data);
      return;
    }
    String line = data == null ? message : '$message\n$data';
    if (level == 'danger') {
      stderr.writeln(line);
    } else {
      stdout.writeln(line);
    }
  }

  /// true when `Dox().config` is set
  // ponytail: dox_core has no public "initialized" flag, so read the
  // late field and catch its error. Use the flag when dox_core has one.
  static bool _doxReady() {
    try {
      Dox().config;
      return true;
    } on Error {
      return false;
    }
  }
}
