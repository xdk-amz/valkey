#!/bin/bash
# Smoke test for the STORE prototype. STORE and its successor MUST share a
# connection, so every pair is piped into ONE valkey-cli. The server is stopped
# with SHUTDOWN NOSAVE over the socket (never signals). Output goes to $OUT.
set -u
SRC=${SRC:-$(cd "$(dirname "$0")/../src" && pwd)}
OUT=${OUT:-${KIROCREW_SCRATCH:-/tmp}/store-smoke}
mkdir -p "$OUT"; rm -f "$OUT/smoke.rdb"
SOCK=$OUT/s.sock; rm -f "$SOCK"
"$SRC/valkey-server" --port 0 --unixsocket "$SOCK" --save "" --appendonly no \
  --dir "$OUT" --dbfilename smoke.rdb --crash-log-enabled no --logfile "$OUT/server.log" &
for i in $(seq 1 50); do [ -S "$SOCK" ] && break; sleep 0.1; done
C() { timeout 10 "$SRC/valkey-cli" -s "$SOCK" "$@"; }
P() { printf '%s\n' "$@" | timeout 10 "$SRC/valkey-cli" -s "$SOCK"; }

echo "== LPOP replay (expect: OK job-17 / REPLAY job-17 / llen 1)"
C rpush jobs job-17 job-18 >/dev/null
P "STORE req:1 PX 60000" "LPOP jobs"
P "STORE req:1 PX 60000" "LPOP jobs"
echo -n "llen: "; C llen jobs
echo "== mismatch (expect: REPLAY / STOREMISMATCH)"
P "STORE req:1 PX 60000" "LPOP other"
echo "== INCRBY replay (expect: OK 1200 / REPLAY 1200 / balance 1200)"
C set balance 1700 >/dev/null
P "STORE req:2 PX 60000" "INCRBY balance -500"
P "STORE req:2 PX 60000" "INCRBY balance -500"
echo -n "balance: "; C get balance
echo "== BLPOP served by LPUSH (expect: OK queue job-99 / REPLAY queue job-99 / llen 0)"
P "STORE req:b PX 60000" "BLPOP queue 5" > "$OUT/blpop1.txt" 2>&1 &
BG=$!; sleep 0.5
C lpush queue job-99 > /dev/null
wait $BG; cat "$OUT/blpop1.txt"
P "STORE req:b PX 60000" "BLPOP queue 5"
echo -n "llen: "; C llen queue
echo "== BLPOP disconnect while blocked (expect: exists 0)"
( printf 'STORE req:d PX 60000\nBLPOP jobs2 10\n'; sleep 0.7 ) | timeout 10 "$SRC/valkey-cli" -s "$SOCK" >/dev/null 2>&1 &
sleep 1.2
echo -n "req:d exists: "; C exists req:d
echo "== BLPOP timeout stored (expect: OK nil / REPLAY nil)"
P "STORE req:t PX 60000" "BLPOP empty 1"
P "STORE req:t PX 60000" "BLPOP empty 1"
echo "== rejection stores nothing (expect: OK err / exists 0)"
P "STORE req:r PX 60000" "NOSUCHCMD x"
echo -n "req:r exists: "; C exists req:r
echo "== MULTI (expect: array stored, REPLAY replays, llen 1)"
C rpush jobs job-A >/dev/null
P "MULTI" "LPOP jobs" "GET balance" "STORE req:m PX 60000" "EXEC"
P "MULTI" "LPOP jobs" "GET balance" "STORE req:m PX 60000" "EXEC"
echo -n "llen: "; C llen jobs
echo "== not storable (expect: OK / error)"
P "STORE req:s PX 60000" "SUBSCRIBE ch"
echo "== blob head + ttl"
C --no-raw get req:2 | head -c 120; echo
echo -n "pttl req:2: "; C pttl req:2
C shutdown nosave >/dev/null 2>&1
wait
echo "== server log errors"; grep -iE 'panic|assert|=== ' "$OUT/server.log" | head -5
