# camel-iceberg: a TLA+ model check

Notes from model-checking the `camel-iceberg` component sketch on
[jbonofre/iceberg, branch `CAMEL`](https://github.com/jbonofre/iceberg/tree/CAMEL/camel/camel-iceberg/src/main)
(commit 234bcff, May 2024), linked from [CAMEL-22172](https://issues.apache.org/jira/browse/CAMEL-22172).

The code is unfinished (the write step is still a `TODO`), so these are design notes, not bug reports.

- [FINDINGS.md](FINDINGS.md): the short write-up (13 findings, the exactly-once lesson, how to reproduce).
- [REPORT.md](REPORT.md): full details and evidence per finding.
- `repro.sh`: re-runs everything (`export JAVA_HOME=<JDK 21>; ./repro.sh`, needs Maven, about 3 minutes).
- `tla/`: the TLA+ models, TLC configurations and outputs. `harness/`: the real-code checks and producer variants.

`src/camel/` is a copy of the component source as checked (Apache License 2.0). `tools/tla2tools.jar` is the TLA+
tools (MIT License). Local paths in the logs are replaced by `<repo>`, `<local>` and `<tmp>`.
