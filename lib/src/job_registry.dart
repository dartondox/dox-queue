import 'job.dart';

/// Turns a job into a JSON map (its constructor parameters).
typedef JobEncoder = Map<String, Object?> Function(DoxJob job);

/// Makes a job again from the map of [JobEncoder].
typedef JobDecoder = DoxJob Function(Map<String, Object?> data);

/// How to save and load one job class. `lib/jobs.g.dart` makes them.
class JobCodec {
  const JobCodec(this.name, this.type, this.encode, this.decode);

  /// the name saved with each queued job
  final String name;
  final Type type;
  final JobEncoder encode;
  final JobDecoder decode;
}

/// Every job class of the app: `jobRegistry` from `lib/jobs.g.dart`.
/// Give it to `QueueConfig(jobs: jobRegistry)`.
class JobRegistry {
  const JobRegistry(this.codecs);

  final List<JobCodec> codecs;

  /// the codec of a job class, or null
  JobCodec? byType(Type type) {
    // ponytail: a list scan, an app has few job classes
    for (JobCodec codec in codecs) {
      if (codec.type == type) return codec;
    }
    return null;
  }

  /// the codec of a saved job name, or null
  JobCodec? byName(String name) {
    for (JobCodec codec in codecs) {
      if (codec.name == name) return codec;
    }
    return null;
  }

  /// the codec of [job]. Throws a [StateError] when its class is not in
  /// the registry.
  JobCodec of(DoxJob job) {
    JobCodec? codec = byType(job.runtimeType);
    if (codec == null) {
      throw StateError(
        '${job.runtimeType} is not in the job registry. Put @Job() above '
        'the class, run `dart run build_runner build`, and set '
        '`QueueConfig(jobs: jobRegistry)` from lib/jobs.g.dart.',
      );
    }
    return codec;
  }
}
