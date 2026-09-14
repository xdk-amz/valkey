#!/bin/bash
# Latency: what does STORE add per operation? Plain (non-ASAN) build.
# Three --pipe phases, EACH doing N logical pops, so wall times are comparable:
#   A: N x (LPOP)                        baseline, 1 command, 0 extra writes
#   B: N x (SET m:i x PX; LPOP)          fair reference: 2 commands, 1 marker
#                                        write, NO reply capture
#   C: N x (STORE req:i PX; LPOP)        the feature: 2 commands, 1 marker
#                                        write, reply capture + header
# B - A is the unavoidable cost of asking for a second command that writes a
# key; C - B is what the STORE machinery itself costs. Output to $OUT.
set -u
SRC=$(cd "$(dirname "$0")/../src" && pwd)
OUT=${KIROCREW_SCRATCH:-/tmp}/store-bench
mkdir -p "$OUT"; rm -f "$OUT/b.rdb"
SOCK=$OUT/b.sock; rm -f "$SOCK"
"$SRC/valkey-server" --port 0 --unixsocket "$SOCK" --save "" --appendonly no \
  --dir "$OUT" --dbfilename b.rdb --crash-log-enabled no --logfile "$OUT/server.log" &
for i in $(seq 1 50); do [ -S "$SOCK" ] && break; sleep 0.1; done
CLI() { timeout 120 "$SRC/valkey-cli" -s "$SOCK" "$@"; }
N=${N:-200000}
fill() { CLI eval "for i=1,tonumber(ARGV[1]) do redis.call('rpush','jobs','v'..i) end return 1" 0 "$1" >/dev/null; }

python3 - "$N" "$OUT" <<'PY'
import sys
n=int(sys.argv[1]); out=sys.argv[2]
def enc(*a):
    s="*%d\r\n"%len(a)
    for x in a: x=str(x); s+="$%d\r\n%s\r\n"%(len(x),x)
    return s
with open(f"{out}/reqA.txt","w") as f:
    for i in range(n): f.write(enc("LPOP","jobs"))
with open(f"{out}/reqB.txt","w") as f:
    for i in range(n): f.write(enc("SET","m:%d"%i,"x","PX","60000")); f.write(enc("LPOP","jobs"))
with open(f"{out}/reqC.txt","w") as f:
    for i in range(n): f.write(enc("STORE","req:%d"%i,"PX","60000")); f.write(enc("LPOP","jobs"))
PY

run() { # file -> wall seconds
  local t0 t1; t0=$(date +%s.%N)
  timeout 180 "$SRC/valkey-cli" -s "$SOCK" --pipe < "$1" >/dev/null 2>&1
  t1=$(date +%s.%N); python3 -c "print(f'{$t1 - $t0:.4f}')"
}

echo "N = $N pops per phase, pipelined via valkey-cli --pipe, plain build"
fill $((N+10)); WA=$(run "$OUT/reqA.txt")
CLI flushall >/dev/null; fill $((N+10)); WB=$(run "$OUT/reqB.txt")
CLI flushall >/dev/null; fill $((N+10)); WC=$(run "$OUT/reqC.txt")
python3 - "$N" "$WA" "$WB" "$WC" <<'PY'
import sys
n=int(sys.argv[1]); wa,wb,wc=map(float,sys.argv[2:5])
us=lambda w:(w/n)*1e6
print(f"A  LPOP only            : {wa:.4f}s  {n/wa/1000:7.1f}k pops/s  {us(wa):6.3f} us/pop")
print(f"B  SET marker + LPOP    : {wb:.4f}s  {n/wb/1000:7.1f}k pops/s  {us(wb):6.3f} us/pop   (+{us(wb-wa):.3f} us vs A: 2nd command + key write)")
print(f"C  STORE + LPOP         : {wc:.4f}s  {n/wc/1000:7.1f}k pops/s  {us(wc):6.3f} us/pop   (+{us(wc-wb):.3f} us vs B: capture + header + fingerprint)")
PY
CLI shutdown nosave >/dev/null 2>&1
wait
