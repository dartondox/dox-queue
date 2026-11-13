import 'job.dart';

/// Turns a job into a JSON map (its constructor parameters).
typedef JobEncoder = Map<String, Object?> Function(DoxJob job);

/// Makes a job again from the map of [JobEncoder].
typedef JobDecoder = DoxJob Function(Map<String, Object?> data);

/// How to save and load one job class. `lib/generated/jobs.g.dart` makes them.
class JobCodec {
  const JobCodec(this.name, this.type, this.encode, this.decode);

  /// the name saved with each queued job
  final String name;
  final Type type;
  final JobEncoder encode;
  final JobDecoder decode;
}

/// Every job class of the app: `jobRegistry` from `lib/generated/jobs.g.dart`.
/// Give it to `QueueConfig(jobs: jobRegistry)`.
class JobRegistry {
  const JobRegistry(this.codecs);

  final List<JobCodec> codecs;

  /// The jobs of Dox packages, eg. the job of `Mail.queue()` of
  /// dox_mailer. Every registry finds them after its own [codecs], so an
  /// app does not add them. A package adds its jobs with [addBuiltIn].
  /// `jobs.g.dart` lists the mail jobs too, so a worker reads them before
  /// `Mail.configure()` runs. Then the codec in [codecs] is found first.
  static List<JobCodec> get builtIn => List<JobCodec>.unmodifiable(_builtIn);

  static final List<JobCodec> _builtIn = <JobCodec>[];

  /// Add [codec] to [builtIn]. A codec with the same name is replaced,
  /// so a second call changes nothing. Start its name with `dox_queue:`:
  /// the builder does not allow that start for app jobs, so it can not
  /// clash.
  static void addBuiltIn(JobCodec codec) {
    _builtIn.removeWhere((JobCodec old) => old.name == codec.name);
    _builtIn.add(codec);
  }

  /// the codec of a job class, or null
  JobCodec? byType(Type type) {
    // ponytail: a list scan, an app has few job classes
    for (JobCodec codec in codecs.followedBy(_builtIn)) {
      if (codec.type == type) return codec;
    }
    return null;
  }

  /// the codec of a saved job name, or null
  JobCodec? byName(String name) {
    for (JobCodec codec in codecs.followedBy(_builtIn)) {
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
        '`QueueConfig(jobs: jobRegistry)` from lib/generated/jobs.g.dart.',
      );
    }
    return codec;
  }
}
