/// The `dox` commands of dox_queue. The `dox` CLI finds this library from
/// the `dox: commands:` key in pubspec.yaml.
library;

import 'package:dox_core/command.dart';

import 'src/commands/queue_table_command.dart';

export 'src/commands/queue_table_command.dart';

/// every command of this package
final List<Command> commands = <Command>[QueueTableCommand()];
