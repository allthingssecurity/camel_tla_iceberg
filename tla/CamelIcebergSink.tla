--------------------------- MODULE CamelIcebergSink ---------------------------
(* Model of the camel-iceberg producer on jbonofre/iceberg@CAMEL (234bcff).
   One Camel exchange carries the rows of one message. The producer writes the rows
   to a new data file and appends it to the table (IcebergProducer.write). Camel
   redelivers a failed exchange up to MaxAttempts times, then dead-letters it.
   Other writers commit to the same table concurrently (Iceberg optimistic concurrency).

   Mode selects which code is modelled:
     "AsIs"       process() as written: write() is never called (IcebergProducer.java:43)
     "Wired"      write() called as written: rows written, failures swallowed (:87-91),
                  toDataFile() on an unclosed writer (:94), a new catalog per call (:49)
     "WiredClose" as "Wired" plus dataWriter.close() before toDataFile (the obvious first fix)
     "Fixed"      close, fail the exchange on a bad row, one shared catalog, and an
                  idempotent commit (skip if a snapshot already carries this exchange id) *)
EXTENDS Naturals, FiniteSets, TLC

CONSTANTS Msgs, Mode, MaxAttempts, MaxExternal

Rows == Msgs \X {1, 2}
RowsOf(m) == {m} \X {1, 2}

VARIABLES
  pc,           \* [Msgs -> {"idle","writing","written","committing","done","retry","dead"}]
  attempts,     \* [Msgs -> Nat]
  bad,          \* rows whose write throws (wrong type / schema mismatch); chosen at Init
  buf,          \* [Msgs -> SUBSET Rows]  rows in the exchange's current data file
  table,        \* [Rows -> Nat]          committed copies of each row
  ver,          \* table snapshot version
  base,         \* [Msgs -> Nat]          version the exchange's commit is based on
  acked,        \* exchanges that returned success to Camel (the source is acknowledged)
  committedBy,  \* exchanges whose append is in a committed snapshot (snapshot summary)
  catalogs,     \* catalogs built and not closed
  external      \* commits by other writers

vars == <<pc, attempts, bad, buf, table, ver, base, acked, committedBy, catalogs, external>>

Init ==
  /\ pc = [m \in Msgs |-> "idle"]
  /\ attempts = [m \in Msgs |-> 0]
  /\ bad \in SUBSET Rows /\ Cardinality(bad) <= 1
  /\ buf = [m \in Msgs |-> {}]
  /\ table = [r \in Rows |-> 0]
  /\ ver = 0
  /\ base = [m \in Msgs |-> 0]
  /\ acked = {}
  /\ committedBy = {}
  /\ catalogs = 0
  /\ external = 0

Fail(m) ==
  pc' = [pc EXCEPT ![m] = IF attempts[m] < MaxAttempts THEN "retry" ELSE "dead"]

\* Camel calls process() (first delivery or redelivery)
Start(m) ==
  /\ pc[m] \in {"idle", "retry"}
  /\ attempts' = [attempts EXCEPT ![m] = @ + 1]
  /\ IF Mode = "AsIs"
       THEN /\ pc' = [pc EXCEPT ![m] = "done"]          \* returns without writing
            /\ acked' = acked \cup {m}
            /\ UNCHANGED <<buf, base, catalogs>>
       ELSE /\ pc' = [pc EXCEPT ![m] = "writing"]
            /\ base' = [base EXCEPT ![m] = ver]          \* catalog.loadTable
            /\ buf' = [buf EXCEPT ![m] = {}]
            /\ catalogs' = IF Mode = "Fixed" THEN 1 ELSE catalogs + 1
            /\ UNCHANGED acked
  /\ UNCHANGED <<bad, table, ver, committedBy, external>>

\* the row loop (IcebergProducer.java:81-92)
WriteRows(m) ==
  /\ pc[m] = "writing"
  /\ IF Mode = "Fixed" /\ RowsOf(m) \cap bad # {}
       THEN /\ Fail(m)                                 \* a bad row fails the exchange
            /\ UNCHANGED buf
       ELSE /\ buf' = [buf EXCEPT ![m] = RowsOf(m) \ bad]  \* bad row swallowed
            /\ pc' = [pc EXCEPT ![m] = "written"]
  /\ UNCHANGED <<attempts, bad, table, ver, base, acked, committedBy, catalogs, external>>

\* dataWriter.toDataFile() (IcebergProducer.java:94)
ToDataFile(m) ==
  /\ pc[m] = "written"
  /\ IF Mode = "Wired"
       THEN Fail(m)                                    \* IllegalStateException: writer not closed
       ELSE pc' = [pc EXCEPT ![m] = "committing"]
  /\ UNCHANGED <<attempts, bad, buf, table, ver, base, acked, committedBy, catalogs, external>>

Apply(m) ==
  /\ table' = [r \in Rows |-> IF r \in buf[m] THEN table[r] + 1 ELSE table[r]]
  /\ ver' = ver + 1
  /\ committedBy' = committedBy \cup {m}

\* table.newAppend().appendFile(..).commit(): Iceberg refreshes and retries an append
\* on conflict, so the base version does not matter for an append. The commit either
\* succeeds, fails cleanly (retries exhausted), or succeeds on the server while the
\* client sees an exception (CommitStateUnknownException).
Commit(m) ==
  /\ pc[m] = "committing"
  /\ \/ /\ Mode = "Fixed" /\ m \in committedBy        \* idempotent: already committed
        /\ pc' = [pc EXCEPT ![m] = "done"]
        /\ acked' = acked \cup {m}
        /\ UNCHANGED <<table, ver, committedBy>>
     \/ /\ ~(Mode = "Fixed" /\ m \in committedBy)
        /\ \/ /\ Apply(m)                               \* ok
              /\ pc' = [pc EXCEPT ![m] = "done"]
              /\ acked' = acked \cup {m}
           \/ /\ Apply(m)                               \* applied, outcome unknown to client
              /\ Fail(m)
              /\ UNCHANGED acked
           \/ /\ Fail(m)                                \* clean failure
              /\ UNCHANGED <<table, ver, committedBy, acked>>
  /\ UNCHANGED <<attempts, bad, buf, base, catalogs, external>>

External ==
  /\ external < MaxExternal
  /\ external' = external + 1
  /\ ver' = ver + 1
  /\ UNCHANGED <<pc, attempts, bad, buf, table, base, acked, committedBy, catalogs>>

Next ==
  \/ \E m \in Msgs : Start(m) \/ WriteRows(m) \/ ToDataFile(m) \/ Commit(m)
  \/ External

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* Properties *)

\* A message Camel was told is done has all its rows in the table.
AckedDurable == \A m \in acked : \A r \in RowsOf(m) : table[r] >= 1

\* No row is in the table twice.
NoDuplicates == \A r \in Rows : table[r] <= 1

\* The producer does not accumulate open catalogs.
CatalogsBounded == catalogs <= 1

\* Used as an "invariant" to ask TLC: can the producer ever commit anything?
\* A violation is GOOD news (a commit is reachable).
NothingEverCommitted == committedBy = {}

\* Every message ends either durably committed and acked, or dead-lettered (loud failure).
AllSettled == \A m \in Msgs : pc[m] \in {"done", "dead"}
OutcomeOK == AllSettled => \A m \in Msgs : pc[m] = "done" => m \in acked /\ \A r \in RowsOf(m) : table[r] >= 1
=============================================================================
