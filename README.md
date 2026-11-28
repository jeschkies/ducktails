# ducktails
Like DuckDB but for logs.

```
ducktails '/var/log/*.log' '{filename=~".*/app\.log"} |= "error" | logfmt | status>=500'
```

## Trying it with Grafana

```
docker compose up --build
open http://localhost:3000/explore
```

A log generator writes a line a second, ducktails serves it on Loki's port, and
Grafana comes with the datasource provisioned. Explore and log panels work;
metric panels answer 501 until a metric query over a range is built, and "Save &
test" reports an error because Grafana's health check sends
`vector(1)+vector(1)`.

## Roadmap

### Fix first

- [x] An unknown parser stage is a parse error rather than being silently
      swallowed, so `{} | json` no longer answers as if it were bare `{}`.
- [ ] `__error__` and `__error_details__`. Nothing sets them, so a row kept
      because its label would not parse cannot be filtered out again, which is
      the `| __error__=""` idiom Loki users reach for.
- [x] Timestamps, read as an RFC3339 prefix of the line, falling back to ingest
      time as lambda-promtail does. Still to do: other formats, a
      `--timestamp-format` flag, and a line index so lines sharing a timestamp
      keep file order.
- [x] Labels resolve against the catalog's schema: any column that is not
      `timestamp`/`line`/`labels`/`value` is a stream label, in the selector, in
      label filters and as a metric query's series key. `filename` is no longer
      special-cased anywhere.
- [ ] Real stream labels, which needs a catalog that has some — see below.

### Log queries

Parsers:

- [x] `| logfmt`
- [ ] `| logfmt --strict`, `--keep-empty`, and label expressions
- [ ] `| json`, including label expressions and nested paths
- [ ] `| regexp "(?P<name>…)"`
- [ ] `| pattern "<_> <method> <path>"`
- [ ] `| unpack`

Line filters:

- [x] `|=`, `!=`, `|~`, `!~`
- [ ] `|>` and `!>`, pattern line filters — the scanner already tokenizes both
- [ ] `or` chaining: `|= "a" or "b"`
- [ ] `ip("…")` matching

Label filters:

- [x] string, number, duration and byte size comparisons
- [ ] composition: `,`, `and`, `or`, parentheses
- [ ] `==` as a numeric equality operator distinct from `=`
- [ ] `ip("…")` and CIDR matching

Formatting and label manipulation:

- [ ] `| line_format "{{.status}} {{.msg}}"` — Go templates, sprig functions
- [ ] `| label_format new=old, new="{{.foo}}"`
- [ ] `| drop` and `| keep`
- [ ] `| decolorize`

Syntax:

- [ ] `#` comments

### Metric queries

Log range aggregations:

- [x] `count_over_time`, `rate`, `bytes_over_time`, `bytes_rate`, as instant
      queries over `(now - range, now]`, one sample per `filename`
- [ ] `absent_over_time` — needs "no rows yields 1", which an aggregate cannot
      produce without a grid of instants

Unwrapped range aggregations:

- [ ] `| unwrap label`, with `duration()`, `duration_seconds()` and `bytes()`
- [ ] `rate`, `rate_counter`, `sum_over_time`, `avg_over_time`, `min_over_time`,
      `max_over_time`, `first_over_time`, `last_over_time`, `stdvar_over_time`,
      `stddev_over_time`, `quantile_over_time`

Aggregation operators:

- [ ] `sum`, `avg`, `min`, `max`, `count`, `stddev`, `stdvar`
- [ ] `topk`, `bottomk`, `approx_topk`, `sort`, `sort_desc`
- [ ] `by` and `without` grouping

Operators and literals:

- [ ] arithmetic: `+`, `-`, `*`, `/`, `%`, `^`
- [ ] comparison, with and without the `bool` modifier
- [ ] logical: `and`, `or`, `unless`
- [ ] `vector(0)`, scalar literals, `label_replace()`. Grafana's datasource
      health check sends `vector(1)+vector(1)`, so this is also what makes "Save
      & test" pass — Explore works without it.
- [ ] `offset` modifier

### Engine

- [x] Range selectors `[5m]`, and an absolute window: `--from`/`--to`, where
      `--to` doubles as the instant a metric query is evaluated at. Unset means
      the whole of the data, not `logcli`'s last hour — a file is not a live
      store.
- [x] `limit` and direction, defaulting to Loki's 100 and backward. A log query
      is ordered by the plan, since "the newest hundred lines" needs it.
- [ ] Range queries: `--from`/`--to`/`--step` over a grid of instants. Sliding
      windows are the hard part — overlapping windows are not a `GROUP BY`
- [ ] Step and interval for range queries

### Server mode

`ducktails serve` answers enough of Loki's HTTP API to browse logs in Grafana.
Metric panels need the step grid below; after that, Loki's gRPC querier protocol
would let it stand in for a querier.

- [x] `/ready`, `/status/buildinfo`, `/labels`, `/label/{name}/values`
- [x] `/query` and `/query_range` for log queries — `streams`, which the output
      layer already emits. GET and form POST, since Grafana sends both.
- [ ] `/query_range` for metric queries, which needs `matrix` and a grid of
      instants, and is what motivates `sum by`/`without`
- [x] A distroless image (`gcr.io/distroless/cc-debian12`) and a compose
      environment with Grafana, a log generator and the datasource provisioned
- [ ] The gRPC querier protocol: `SchedulerForQuerier.QuerierLoop`, typed
      `queryrange.QueryRequest` work, results returned to the frontend address
      it hands over. A static scheduler address avoids needing dskit's ring.

### Instrumentation

Deferred on purpose: the server works without it, and every OTel layer sits on
top of these two crates anyway.

- [ ] `tracing` and `tracing-subscriber` with `EnvFilter`, replacing the
      `eprintln!`s. DataFusion instruments with `log` rather than `tracing`, so
      the bridge is what surfaces planner diagnostics.
- [ ] Spans around opening a catalog, planning and execution — where a slow
      query's time actually goes, which a benchmark cannot tell you.
- [ ] OTLP export and `/metrics`. The `opentelemetry` crates are all 0.x and
      must move together: a mismatched pair gives two registries and spans that
      silently go nowhere.

### Catalogs

A catalog is where lines come from. Each one is a DataFusion `TableProvider`
presenting the same contract — `timestamp`, `line`, `labels`, plus a column per
stream label — so the planner resolves labels the same way whatever it is reading
(see the labels item under "Fix first").

- [x] **Files**: a path or glob, one `filename` label, timestamps read from the
      line.
- [x] **Parquet**, locally, via a `file://` URL. The dataset needs `timestamp`
      and `line` columns; every other column is a stream label, so a selector
      matches on it and a metric query groups by it. Both are cast into the
      shapes the engine works in, so a `Utf8` line and a millisecond timestamp
      read fine. Built on `ListingTable` with `ParquetFormat`, so listing,
      statistics pruning, projection and predicate pushdown come from DataFusion.
- [ ] **Parquet from object storage** — `s3://` and friends, once the object
      store is registered from catalog config.
- [ ] Hive-partitioned layouts (`env=prod/service=api/…`) as label columns, which
      prune whole directories before a byte is read — the fastest selector we can
      have. `ListingOptions::with_table_partition_cols` is the hook; it is not
      set yet, so such a layout currently exposes no labels for the partitions.
- [ ] A column mapping, for tables whose columns are not already called
      `timestamp` and `line`.
- [ ] **Loki HTTP API**: query another Loki instance. The selector and line
      filters translate back into LogQL and push down through
      `supports_filters_pushdown`, so the remote does the work.
      `/loki/api/v1/labels` and `/series` give the label names to advertise as
      columns — treated as a hint, since the set is time-scoped: an unknown name
      still falls back to the `labels` list rather than failing.
- [ ] **Loki index and storage**: read Loki's own TSDB index and chunks from
      object storage directly, without a Loki to ask. The index is the selector's
      pruning structure, which is the whole point of going underneath the API.
- [ ] **journald**, whose fields (`_SYSTEMD_UNIT`, `PRIORITY`, …) are its labels.
- [ ] **Iceberg** for file listing only: its manifests carry per-file partition
      values and column statistics, so files can be pruned from metadata rather
      than by listing directories. The reading stays with DataFusion's Parquet
      support. Blocked on versions for now — `iceberg-datafusion` 0.11 wants
      DataFusion 54 and Arrow 58, against our 55 and 59.

Configuration, still to design:

- [ ] The source argument accepts a path or glob, a URI (`s3://…`,
      `loki+https://…`, `journal:`), or a name looked up in a catalogs file.
- [ ] `catalogs.yaml` for what does not fit a flag: credentials by environment
      reference, and the column mapping for Parquet and other column-mapped
      tables — which column is the timestamp, which is the line, which columns
      are labels.
- [ ] An async catalog-resolution step before planning: register object stores,
      infer or read schemas, build the `TableSource`. `plan()` stays synchronous
      and does no I/O.
- [ ] Per-catalog defaults for `--now`: the newest record for an archive, wall
      clock for a live store, instead of one global rule.

A catalog is deliberately **not** a stream label. `{catalog=~"prod-.*"}` would
make the planner resolve data sources from a regex, and `{catalog!="files"}` has
no sensible meaning at all. Loki keeps the tenant out of the selector too — it
travels in `X-Scope-OrgID` — and this follows that. Once several catalogs can be
queried at once, exposing `catalog` as an ordinary column to *filter* on is fine;
choosing the source with the selector is what is not.

### Performance, once correctness is there

- [ ] `label_duration` re-parses per row, so `| duration>10ms` costs about twice
      `| status>=500`, which uses a vectorized `try_cast`
- [ ] `filename` is copied per row (`TODO: can save memory here`)
      worth attributing with a heap profiler
