/// The build_runner builder of dox_queue. Apps do not import it.
library;

import 'package:build/build.dart';

import 'src/builder/job_registry_builder.dart';

export 'src/builder/job_registry_builder.dart';

/// Writes `lib/jobs.g.dart` with every `@Job()` class of the app.
Builder buildJobRegistry(BuilderOptions options) {
  return const JobRegistryBuilder();
}
