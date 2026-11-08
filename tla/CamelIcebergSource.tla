-------------------------- MODULE CamelIcebergSource --------------------------
(* Extension of CamelIcebergSink: redelivery by the SOURCE, not by Camel's error handler.
   A route  from(source).to("iceberg:...").to(next)  : the Iceberg commit succeeds, then
   - a later step fails and the source does not acknowledge (file consumer leaves the file,
     JMS transacted rollback, Kafka offset not committed), or
   - the JVM crashes after the commit and before the acknowledgement.
   The source then delivers the same message again as a NEW exchange (new exchange id).
   The producer is the "fixed" one of CamelIcebergSink (close, fail on bad row, one catalog);
   Dedup selects how it recognises an earlier commit:
     "None"       no check (the producer as written, once completed)
     "ExchangeId" snapshot summary carries the exchange id (fixes Camel-internal redelivery, L2)
     "MessageKey" snapshot summary carries a key stable across source redeliveries
                  (file name + modification time, JMS message id, Kafka topic/partition/offset) *)
EXTENDS Naturals, FiniteSets

CONSTANTS Msgs, Dedup, MaxDeliveries

VARIABLES
  pc,         \* [Msgs -> {"queued","inflight","committed","acked"}]
  exch,       \* [Msgs -> Nat]  exchange id counter of the current delivery (new per source delivery)
  deliveries, \* [Msgs -> Nat]
  table,      \* [Msgs -> Nat]  committed copies of the message's rows
  marks       \* set of <<m, exchangeId>> recorded in snapshot summaries

vars == <<pc, exch, deliveries, table, marks>>

Init ==
  /\ pc = [m \in Msgs |-> "queued"]
  /\ exch = [m \in Msgs |-> 0]
  /\ deliveries = [m \in Msgs |-> 0]
  /\ table = [m \in Msgs |-> 0]
  /\ marks = {}

AlreadyCommitted(m) ==
  CASE Dedup = "None"       -> FALSE
    [] Dedup = "ExchangeId" -> <<m, exch[m]>> \in marks
    [] Dedup = "MessageKey" -> \E p \in marks : p[1] = m

\* the source hands the message to the route as a new exchange
Deliver(m) ==
  /\ pc[m] = "queued"
  /\ deliveries[m] < MaxDeliveries
  /\ deliveries' = [deliveries EXCEPT ![m] = @ + 1]
  /\ exch' = [exch EXCEPT ![m] = @ + 1]
  /\ pc' = [pc EXCEPT ![m] = "inflight"]
  /\ UNCHANGED <<table, marks>>

\* the Iceberg step: skip if already committed, else append (commit succeeds)
IcebergStep(m) ==
  /\ pc[m] = "inflight"
  /\ IF AlreadyCommitted(m)
       THEN UNCHANGED <<table, marks>>
       ELSE /\ table' = [table EXCEPT ![m] = @ + 1]
            /\ marks' = marks \cup {<<m, exch[m]>>}
  /\ pc' = [pc EXCEPT ![m] = "committed"]
  /\ UNCHANGED <<exch, deliveries>>

\* the rest of the route succeeds and the source is acknowledged
Ack(m) ==
  /\ pc[m] = "committed"
  /\ pc' = [pc EXCEPT ![m] = "acked"]
  /\ UNCHANGED <<exch, deliveries, table, marks>>

\* a later step fails, or the JVM crashes, before the acknowledgement: the source keeps the message
NotAcked(m) ==
  /\ pc[m] = "committed"
  /\ pc' = [pc EXCEPT ![m] = "queued"]
  /\ UNCHANGED <<exch, deliveries, table, marks>>

Next == \E m \in Msgs : Deliver(m) \/ IcebergStep(m) \/ Ack(m) \/ NotAcked(m)
Spec == Init /\ [][Next]_vars

NoDuplicates == \A m \in Msgs : table[m] <= 1
AckedDurable == \A m \in Msgs : pc[m] = "acked" => table[m] >= 1
=============================================================================
