# dox-queue

The queue packages of [Dox](https://dartondox.dev), a web backend framework for Dart.

| Package | What it is |
| --- | --- |
| [`dox_queue`](packages/dox-queue) | Background jobs: the Redis and sync drivers, retries, delays, chains, batches and workers. |
| [`dox_queue_database`](packages/dox-queue-database) | The database driver, the batch store and `afterCommit` for `dox_queue`, on dox_orm. |

Both share one version. Docs: [dartondox.dev](https://dartondox.dev/latest/digging-deeper/queue/).

## Local development

The repo is a [Dart pub workspace](https://dart.dev/tools/pub/workspaces).
One `dart pub get` at the root links all packages.

`ci/use-siblings.sh` clones the other Dox repos next to this one and
writes a `pubspec_overrides.yaml` at the root (it is in `.gitignore`), so
the packages use their newest code, not pub.dev.

## Tests

SQLite needs no server. Postgres, MariaDB and Redis run when their port
is set (`PG_PORT`, `MYSQL_PORT`, `REDIS_PORT`):

```bash
cd packages/dox-queue-database
REDIS_PORT=6379 dart test
```
