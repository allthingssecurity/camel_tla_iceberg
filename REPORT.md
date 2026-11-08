# camel-iceberg (jbonofre/iceberg, branch CAMEL @ 234bcff, 2024-05-17): Specula-style check

Method: a TLA+ model of the producer's intended write protocol, searched by TLC, then each violation confirmed on the
real classes, compiled unchanged, with a real HadoopCatalog on local disk and real Camel routes with redelivery.

Versions:
- Camel 4.4.2: the version the branch pins (`gradle/libs.versions.toml`).
- Iceberg: the branch does not pin a release. Its Camel module builds against the branch's own in-tree Iceberg
  modules, which are upstream `main` at 5aa0d3b (2024-04-30), the 1.6 development line. The Camel commits on top
  are from 2024-05-15/17.
- The harness therefore used Iceberg 1.5.2, the nearest release.
- 5aa0d3b has diverged from 1.5.2 (170 commits that 1.5.2 lacks; 1.5.2 has 6 release-branch backports) and is
  216 commits behind 1.6.0.
- So the harness was also re-run on Iceberg 1.6.0 (`repro16-*.log`). Every result is identical on both.
- The Iceberg code these findings depend on does not change in substance across 1.5.2 → 5aa0d3b → 1.6.0:
  - `CatalogUtil.buildIcebergCatalog`: only an unrelated helper added, and a one-word call change in 1.6.0.
  - `SnapshotProducer.commit`: still rethrows `CommitStateUnknownException`; only re-indented in 1.6.0.
  - `DataWriter` and the Parquet writer classes: unchanged in both ranges.
- Hidden ("masked") bugs: tested by applying only the obvious first fix (close the writer, wire in `write()`).
- Control: a fixed producer.

The model was run in four modes:
- AsIs: the code as written.
- Wired: `write()` called as written.
- WiredClose: plus `dataWriter.close()`.
- Fixed.

## Files
- Model: `tla/CamelIcebergSink.tla`, configs `tla/MC_<mode>_<property>.cfg`, TLC output `tla/out_*.txt`
- Real-code harness: `harness/test/Harness.java`
- Producer variants: `harness/close/IcebergProducer.java` (first fix only), `harness/fixed/IcebergProducer.java` (control)
- Logs: `repro-{orig,close,fixed}.log` (Iceberg 1.5.2), `repro16-{orig,close,fixed}.log` (Iceberg 1.6.0)

## TLC results (2 messages, 2 rows each, 1 bad row possible, 2 delivery attempts, 1 concurrent external commit)
| Mode | AckedDurable | NoDuplicates | CatalogsBounded | can commit? |
|---|---|---|---|---|
| AsIs | VIOLATED (2 states) | holds | holds | never |
| Wired | holds (nothing is ever acked) | holds | VIOLATED | never |
| WiredClose | VIOLATED (5 states: bad row swallowed, partial file committed, acked) | VIOLATED (9 states: commit applied, reply lost, redelivery appends again) | VIOLATED | yes |
| Fixed | holds (3,925 states) | holds | holds | yes |

The concurrent external commit never broke anything: Iceberg's append retry handles it.

## Confirmed on real code
Each finding is marked by how it shows up:
- "as written": happens with the code exactly as it is.
- "masked": hidden by an earlier bug, and shows up once that bug is fixed.

| # | Finding | Kind | Evidence |
|---|---|---|---|
| C1 | `IcebergComponent.catalog` has `@Metadata` but no setter, so Camel cannot configure it (PropertyBindingException) | as written | repro-orig C1 |
| C2 | `IcebergCatalog.catalog()` fails for every configuration. `properties()` always puts all keys, with nulls: type only gives an NPE on `catalog-impl=null`; implementation only gives an NPE on `type=null`; both set gives "both type and catalog-impl are set" | as written | repro-orig C2 (3/3 fail) |
| P1 | `process()` never calls `write()`. The exchange succeeds and nothing is written (silent loss) | as written | repro-orig P1: succeeded, 0 snapshots |
| P2 | A missing `ICEBERG_TABLE` header gives a NullPointerException, not a clear error | as written | repro-orig P2 |
| P3 | `write()` can never commit: `toDataFile()` on an unclosed writer gives "Cannot create data file from unclosed writer" | masked by C2 | repro-orig P3 |
| P4 | A new catalog for every message, never closed (resource leak) | as written in `write()` | repro-orig P3, repro-close L3 |
| L1 | A bad row's write exception is swallowed. The remaining rows are committed into a corrupt data file and the exchange is reported as success. Afterwards reading the table fails (`ParquetDecodingException`): **one bad message makes the whole table unreadable** | masked by P1 + P3 | repro-close L1 |
| L2 | Commit applied but reply lost (`CommitStateUnknownException`), then Camel redelivers and the rows are **appended twice** | masked by P1 + P3 | repro-close L2: [1:a, 1:a, 2:b, 2:b] |
| L3 | Partitioned tables are rejected by Iceberg ("Partition must not be null"). A loud failure, so a limitation, not silent | masked | repro-close L4 |

Control (`fixed`):
- L1: the exchange fails and the table is untouched.
- L2: exactly once (the snapshot summary carries the exchange id).
- 1 catalog in total.
- Partitioned tables are rejected with a clear message.

Not a problem: no orphan data files were left by the failed attempts in P3, because the Parquet writer is never flushed.

## Round 2: redelivery by the message source (same shape as L2, one level up)

Model: `tla/CamelIcebergSource.tla` (TLC output `tla/outS_*.txt`). It covers a route `from(source).to("iceberg:...").to(next)`.
The Iceberg commit succeeds, but the source is not acknowledged, because either:
- a later step fails, or
- the JVM dies before the acknowledgement.

The source then delivers the same message again as a NEW exchange.

| How the producer recognises an earlier commit | NoDuplicates (169 states) |
|---|---|
| none (the code as written, once completed) | VIOLATED (6 states) |
| exchange id in the snapshot summary (the `fixed` control above) | VIOLATED (6 states): the redelivery has a new exchange id |
| a key stable across source deliveries (file name + last-modified, JMS message id, Kafka topic/partition/offset) | holds |

Real code: `harness/test/SourceRedelivery.java` with a real Camel file consumer. The rows are `1,a` and `2,b`.
Both Iceberg 1.5.2 and 1.6.0 give the same result (`repro-source-redelivery.log`).

| # | Finding | close (first fix only) | fixed (exchange-id marker) | keyed (stable message key) |
|---|---|---|---|---|
| L4 | A later route step fails once, then the file consumer re-polls the file and the rows are **appended twice** | BUG [1:a,1:a,2:b,2:b] | **BUG** [1:a,1:a,2:b,2:b] | ok [1:a,2:b] |
| L5 | The JVM is halted right after the Iceberg commit; after restart the file is re-polled and the rows are **appended twice** | BUG | **BUG** | ok |

### Correction to the control above
The `fixed` producer's exchange-id marker only covers Camel's own redelivery (L2). It does NOT give exactly-once
end to end. A correct component must record a key that survives source redelivery, and commit it in the same
Iceberg snapshot as the data. That key depends on the source:
- a Kafka offset;
- a JMS message id;
- a file name plus its last-modified time.

The source must be acknowledged only after that commit. This is the scheme Iceberg's Kafka Connect sink uses: offsets
in the snapshot summary. The `keyed` variant (`harness/keyed/IcebergProducer.java`) shows the effect, through a
header `ICEBERG_DEDUP_KEY`.

What it does not handle:
- The key check scans every snapshot. A real component would keep a bounded watermark per source partition instead.
- Snapshot expiry can drop old markers.

## Round 3: is the keyed fix correct? Concurrent copies of one message (L6)
Model: `tla/CamelIcebergConcurrent.tla`. Two consumers hold the same message (same key); each checks the snapshot
summaries, then appends.
- With a plain check-then-commit, `NoDuplicates` is VIOLATED (5 states: both check, both commit).
- With the check inside the commit (a conditional commit), it holds.

Real code: `harness/test/ConcurrentKey.java`, the keyed producer, 2 threads, the same `ICEBERG_DEDUP_KEY`, 30 trials:
**duplicate rows in 30 of 30** (`repro-concurrent-key.log`). Iceberg retries a conflicting append by re-applying it
to the new head, so the caller's earlier key check is never repeated.

The keyed fix is therefore correct only with one in-flight copy per message. A complete design needs one of:
- a compare-and-set loop: commit with retries off, and on conflict refresh, re-check and retry. Shown in the model
  only; not implemented here;
- a single writer per source partition.
