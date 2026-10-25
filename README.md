# dox_queue

Background jobs for the Dox framework: Redis, database and sync drivers, retries, delays and workers.

Work in progress. Not on pub.dev yet.

Part of [Dox](https://dartondox.dev), a fast backend framework for Dart.

## Development

For local work against the Dox repo next to this one, add a `pubspec_overrides.yaml` (it is gitignored):

```yaml
dependency_overrides:
  dox_core:
    path: ../dox/packages/dox-core
```

## Release

Set `version` in `pubspec.yaml`, add it to `CHANGELOG.md`, merge to `main`, then push a `v<version>` tag.
The Release workflow publishes the package to pub.dev.

## License

MIT. See [LICENSE](LICENSE).
