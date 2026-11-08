------------------------ MODULE CamelIcebergConcurrent ------------------------
(* The "keyed" fix: before appending, look for the message key in the snapshot summaries; if
   absent, append data + key. Two consumers hold a copy of the SAME message at the same time
   (JMS redelivery to another consumer while the first is still working, two nodes polling one
   folder). Iceberg retries an append on a conflict by re-applying it to the new head, without
   re-running the caller's key check. Atomic = TRUE models a check done inside the commit. *)
EXTENDS Naturals
CONSTANTS Consumers, Atomic
VARIABLES pc, rows, marked
vars == <<pc, rows, marked>>
Init == pc = [c \in Consumers |-> "check"] /\ rows = 0 /\ marked = FALSE
Check(c) == /\ pc[c] = "check"
            /\ pc' = [pc EXCEPT ![c] = IF marked THEN "skip" ELSE "commit"]
            /\ UNCHANGED <<rows, marked>>
Commit(c) == /\ pc[c] = "commit"
             /\ IF Atomic /\ marked                       \* conditional commit: re-check at commit time
                  THEN UNCHANGED <<rows, marked>>
                  ELSE /\ rows' = rows + 1 /\ marked' = TRUE
             /\ pc' = [pc EXCEPT ![c] = "done"]
Next == \E c \in Consumers : Check(c) \/ Commit(c)
Spec == Init /\ [][Next]_vars
NoDuplicates == rows <= 1
=============================================================================
