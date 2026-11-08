# camel-iceberg: what a model check found

**Target:** the `camel-iceberg` component sketch on `jbonofre/iceberg`, branch `CAMEL` (commit 234bcff, 17 May 2024).
It has 6 Java files and 461 lines: 260 lines of code, or 208 not counting imports.
It is linked from the open [CAMEL-22172](https://issues.apache.org/jira/browse/CAMEL-22172) but not yet proposed as a pull request. It is unfinished: the write step is still a `TODO`.

**Method:** the approach Specula uses, done by hand.
1. Write a TLA+ model of what the producer is meant to do: write a message's rows to a file, commit them to the
   table, with Camel and the message source retrying failures and other writers committing at the same time.
2. Let the TLC model checker search every order of events, failure and retry.
3. Confirm every violation on the real code. The branch's classes were compiled unchanged against Camel 4.4.2 (the
   version the branch pins). The branch builds against its own Iceberg modules, upstream `main` at 5aa0d3b (30 April
   2024, the 1.6 development line), so it was run on Iceberg 1.5.2, the nearest release, and on 1.6.0, which bracket
   that base.
4. Use a real table on local disk and real Camel routes.
5. For bugs hidden behind earlier ones, apply only the obvious first fix and test again.
6. Use a corrected producer as the control.

**Result:** 13 problems, all reproduced, with identical results on both Iceberg versions.

## Bugs in the code as written
| # | Problem | Kind |
|---|---|---|
| P1 | The producer never writes but reports success: **silent data loss** | unfinished code |
| C2 | `IcebergCatalog.catalog()` fails for **every** configuration tried with the Hadoop catalog (it passes null values to Iceberg) | configuration bug |
| C1 | The catalog cannot be configured through Camel (no setter) | configuration bug |
| P3 | `write()` can never commit (it asks for the file before closing the writer) | API misuse |
| P2 | A missing table header crashes with `NullPointerException` | input validation |
| P4 | A new catalog for every message, never closed | resource leak |

## Problems hidden until the bugs above are fixed (the important ones)
| # | Problem | Kind |
|---|---|---|
| L1 | One bad row: the error is swallowed, a corrupt file is committed, the message reports success, and **the whole table becomes unreadable** | error handling / atomicity |
| L2 | The commit succeeds but the reply is lost, then **Camel's retry writes the rows twice** | distributed systems (exactly-once) |
| L4 | The commit succeeds, then a **later route step fails**, and the **source redelivers** and the rows are written twice | distributed systems (exactly-once) |
| L5 | The commit succeeds, then the **app crashes** before acknowledging the source; after restart the rows are written twice | distributed systems (exactly-once) |
| L6 | Even with a stable-key check, **two copies of one message at the same time** are both written (30/30 trials) | distributed systems (check-then-commit race) |
| L3 | Partitioned tables are rejected (a loud error, so a limitation) | missing feature |

## The main lesson (L2, L4, L5, L6)
Checking "was this exchange already committed?" only fixes Camel's own retry (L2). A redelivery from the source
arrives as a new exchange, so that check misses it.

Storing **a stable source key in the same Iceberg commit** as the data, and acknowledging the source only after
that commit, fixes L4 and L5 when **one copy of a message is in flight at a time**. Examples of a key: the Kafka
offset, the JMS message id, the file name plus its modification time. The model showed it (169 states) and the real
code confirmed it, including after a real JVM crash.

It is **not enough on its own**:
- **L6, confirmed:** two copies of the same message processed at the same time are both written: duplicates in
  30 of 30 real trials. This happens with JMS redelivery to a second consumer, or two nodes on one folder.
  "Check for the key, then commit" is two steps, and Iceberg's automatic commit retry re-applies the append to the
  new table head without re-running the check. The check must be part of the commit itself. Two ways:
  - commit without Iceberg's automatic retry, and on conflict refresh, re-check and try again (a compare-and-set
    loop; this holds in the model);
  - guarantee a single writer per source partition, as Iceberg's Kafka Connect sink does with one coordinator.
- **Not tested, from reading:**
  - the key must be unique per message. A file name plus a coarse timestamp can match a new file, which would
    then be silently skipped (loss);
  - the markers must outlive the redelivery window. Snapshot expiry removes old summaries;
  - scanning every snapshot does not scale. Kafka Connect keeps one offset watermark per partition instead.

**Why not Camel's Idempotent Consumer?** Its repository is not written in the same commit as the Iceberg data, so a crash between the two still leaves a gap (L5), and two concurrent copies can both pass it (L6). The source key has to be stored in the Iceberg commit itself.

## Is anything fileable?
Not as bug reports: this is an unfinished personal branch. It is useful as design notes for the author or for anyone
building a Camel Iceberg component, and the four exactly-once traps (L2, L4, L5, L6) are the parts that matter.

**Labels in the logs:** the Round 1 logs (`repro-*.log`, `repro16-*.log`, `repro-all.log`) use their own numbering: log `[L3]` is P4 here (a catalog per message), and log `[L4]` is L3 here (partitioned tables). L4 and L5 are in `repro-source-redelivery.log`, L6 in `repro-concurrent-key.log`.

## Reproduce
`export JAVA_HOME=<JDK 21>; ./repro.sh`. It needs Maven, downloads the jars on first run, and takes about 3 minutes.

## What was checked
- 461 lines of component source (all of it).
- Three TLA+ models (243 lines) under 24 TLC configurations.
- A real-code harness (461 lines).
- 3 producer variants.
- 2 Iceberg versions.

## Files
| Path | What |
|---|---|
| `FINDINGS.md` | this summary |
| `REPORT.md` | full details and evidence per finding |
| `repro.sh` | one command to re-run everything |
| `repro-all.log` | output of the last full run |
| `tla/CamelIcebergSink.tla`, `tla/CamelIcebergSource.tla`, `tla/CamelIcebergConcurrent.tla` | the models |
| `tla/MC*.cfg` | TLC configurations |
| `tla/out*.txt` | TLC output with counterexample traces |
| `harness/test/Harness.java` | real-code checks: configuration, producer, L1–L3 |
| `harness/test/SourceRedelivery.java` | real file consumer: L4 and L5 |
| `harness/close/` | producer with only the first fix (to expose hidden bugs) |
| `harness/fixed/` | producer that deduplicates by exchange id (fixes L1, L2; not L4, L5) |
| `harness/keyed/` | producer that deduplicates by a stable source key (fixes L4, L5; not L6) |
| `harness/test/ConcurrentKey.java` | two concurrent copies of one message: L6 |
| `src/camel/` | the component source as checked (Apache License 2.0) |
| `deps/pom.xml` | the jar versions used |
| `tools/tla2tools.jar` | the TLC model checker |
