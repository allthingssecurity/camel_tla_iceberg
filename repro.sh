#!/bin/bash
# Reproduce every camel-iceberg finding from scratch: TLC models + real-code runs on Iceberg 1.5.2 and 1.6.0.
# Usage: ./repro.sh        (needs JDK 21, Maven; downloads jars on first run; about 2-3 minutes)
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"; cd "$HERE"
: "${JAVA_HOME:?set JAVA_HOME to a JDK 21}"
J="$JAVA_HOME/bin/java"; JC="$JAVA_HOME/bin/javac"
TLA="${TLA:-$HERE/tools/tla2tools.jar}"
OUT="$HERE/out"; rm -rf "$OUT"; mkdir -p "$OUT"
SRC=$(find src/camel -name '*.java')

echo "== TLC: producer model (tla/CamelIcebergSink.tla)"
for mode in AsIs Wired WiredClose Fixed; do for inv in AckedDurable NoDuplicates CatalogsBounded NothingEverCommitted; do
  r=$("$J" -Xmx1g -cp "$TLA" tlc2.TLC -workers 2 -deadlock -metadir "$OUT/tlc" -config "tla/MC_${mode}_${inv}.cfg" tla/CamelIcebergSink.tla 2>&1)
  printf "   %-10s %-21s %s\n" "$mode" "$inv" "$(echo "$r" | grep -o 'Invariant .* is violated' || (echo "$r" | grep -q 'No error has been found' && echo holds) || echo ERROR)"
done; done
echo "== TLC: concurrent copies model (tla/CamelIcebergConcurrent.tla)"
for a in FALSE TRUE; do
  r=$("$J" -Xmx1g -cp "$TLA" tlc2.TLC -deadlock -metadir "$OUT/tlc" -config "tla/MCC_Atomic_$a.cfg" tla/CamelIcebergConcurrent.tla 2>&1)
  printf "   Atomic=%-5s NoDuplicates          %s\n" "$a" "$(echo "$r" | grep -o 'Invariant .* is violated' || (echo "$r" | grep -q 'No error has been found' && echo holds) || echo ERROR)"
done
echo "== TLC: source-redelivery model (tla/CamelIcebergSource.tla)"
for d in None ExchangeId MessageKey; do for inv in NoDuplicates AckedDurable; do
  r=$("$J" -Xmx1g -cp "$TLA" tlc2.TLC -workers 2 -deadlock -metadir "$OUT/tlc" -config "tla/MCS_${d}_${inv}.cfg" tla/CamelIcebergSource.tla 2>&1)
  printf "   %-10s %-21s %s\n" "$d" "$inv" "$(echo "$r" | grep -o 'Invariant .* is violated' || (echo "$r" | grep -q 'No error has been found' && echo holds) || echo ERROR)"
done; done

for IV in 1.5.2 1.6.0; do
  D="$OUT/iceberg-$IV"; mkdir -p "$D/lib" "$D/orig" "$D/test"
  sed "s|<version>1.5.2</version>|<version>$IV</version>|g" deps/pom.xml > "$D/pom.xml"
  (cd "$D" && mvn -q -B dependency:copy-dependencies -DoutputDirectory=lib) || { echo "maven failed"; exit 1; }
  CP=$(ls "$D"/lib/*.jar | tr '\n' ':')
  "$JC" -nowarn -d "$D/orig" -cp "$CP" $SRC 2>&1 | grep -v '^Note'          # the branch's classes, unchanged
  for v in close fixed keyed; do mkdir -p "$D/$v"; "$JC" -nowarn -d "$D/$v" -cp "$D/orig:$CP" harness/$v/IcebergProducer.java 2>&1 | grep -v '^Note'; done
  "$JC" -nowarn -d "$D/test" -cp "$D/orig:$CP" harness/test/Harness.java harness/test/SourceRedelivery.java harness/test/ConcurrentKey.java 2>&1 | grep -v '^Note'
  echo "== Real code, Iceberg $IV, Camel 4.4.2"
  for v in orig close fixed; do
    PRE=""; [ $v != orig ] && PRE="$D/$v:"
    echo "  -- producer: $v"
    "$J" -Xmx1g -cp "${PRE}$D/test:$D/orig:$CP" Harness $v 2>&1 | grep -E '^\[|BUG|  ok|      ' | cut -c1-260
  done
  echo "  -- source redelivery (real file consumer)"
  for v in close fixed keyed; do
    W="$OUT/w-$IV-$v-later"; mkdir -p "$W"
    "$J" -Xmx1g -cp "$D/$v:$D/test:$D/orig:$CP" SourceRedelivery $v later "$W" 2>&1 | grep -E 'BUG|  ok' | cut -c1-260
    W="$OUT/w-$IV-$v-crash"; mkdir -p "$W"
    "$J" -Xmx1g -cp "$D/$v:$D/test:$D/orig:$CP" SourceRedelivery $v crash1 "$W" >/dev/null 2>&1
    "$J" -Xmx1g -cp "$D/$v:$D/test:$D/orig:$CP" SourceRedelivery $v crash2 "$W" 2>&1 | grep -E 'BUG|  ok' | cut -c1-260
  done
  echo "  -- two concurrent copies of one message (keyed producer)"
  "$J" -Xmx1g -cp "$D/keyed:$D/test:$D/orig:$CP" ConcurrentKey 30 2>&1 | grep -E 'BUG|  ok'
done
