#!/usr/bin/env python3
"""Server-side latency of SPOP / SRANDMEMBER on large sets, per binary.

All servers run at once and are measured in interleaved rounds, so drift in the
host's load hits every binary alike; the median round is reported. SPOP runs on
a fresh COPY each round and never pops more than a tenth of the live members.

Shapes (n = --n members, hashtable encoding):
  plain     no member TTLs
  shared    every member shares ONE far-future deadline (#27)
  allttl55  every member TTL'd, 55% expired-unreclaimed (#41)
  mixed55   45% persistent live, 55% expired-unreclaimed
  allttl40  every member TTL'd, 40 live, the rest expired-unreclaimed
  mixed40   40 persistent live, the rest expired-unreclaimed
Reports usec/call from INFO commandstats (server time only).
usage: bench.py --server NAME=BIN [--server NAME=BIN ...] [--n N] [--reps R] [--rounds K]
"""
import argparse
import os
import statistics
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import setfuzz  # noqa: E402

CMDS = [
    ("SRANDMEMBER", []),
    ("SRANDMEMBER", ["10"]),
    ("SRANDMEMBER", ["-10"]),
    ("SPOP", ["1"]),
    ("SPOP", ["10"]),
]
LIVE = {"plain": 1.0, "shared": 1.0, "allttl55": 0.45, "mixed55": 0.45, "allttl40": None, "mixed40": None}


def build(c, key, n, shape, far):
    c.cmd("DEL", key)
    members = ["m%d" % i for i in range(n)]
    for i in range(0, n, 5000):
        c.cmd("SADD", key, *members[i:i + 5000])
    if shape == "plain":
        return n
    if shape == "shared":
        for i in range(0, n, 5000):
            chunk = members[i:i + 5000]
            c.cmd("SPEXPIREAT", key, far, "MEMBERS", len(chunk), *chunk)
        return n
    nlive = 40 if shape.endswith("40") else n * 45 // 100
    live, hidden = members[:nlive], members[nlive:]
    if shape.startswith("allttl"):
        for i in range(0, len(live), 5000):
            chunk = live[i:i + 5000]
            c.cmd("SPEXPIREAT", key, far + (i // 5000) * 60000, "MEMBERS", len(chunk), *chunk)
    for i in range(0, len(hidden), 5000):
        chunk = hidden[i:i + 5000]
        c.cmd("SPEXPIRE", key, 1, "MEMBERS", len(chunk), *chunk)
    return nlive


def usec_per_call(c, name):
    for line in c.cmd("INFO", "commandstats").splitlines():
        if line.startswith("cmdstat_%s:" % name.lower()):
            for kv in line.split(":", 1)[1].split(","):
                k, v = kv.split("=")
                if k == "usec_per_call":
                    return float(v)
    return float("nan")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--server", action="append", required=True)
    ap.add_argument("--n", type=int, default=1000000)
    ap.add_argument("--reps", type=int, default=100)
    ap.add_argument("--rounds", type=int, default=5)
    ap.add_argument("--port", type=int, default=28000)
    ap.add_argument("--shapes", default="plain,shared,allttl55,mixed55,allttl40,mixed40")
    a = ap.parse_args()
    base = os.path.join(os.environ["KIROCREW_SCRATCH"], "fuzz", "run")
    shapes = a.shapes.split(",")
    names = [spec.split("=", 1)[0] for spec in a.server]
    servers, clients = [], []
    try:
        for idx, spec in enumerate(a.server):
            name, binary = spec.split("=", 1)
            s = setfuzz.Server(binary, base, "bench-%s" % name, a.port + idx,
                               extra=["--enable-debug-assert", "no"]).start()
            servers.append(s)
            c = s.client(timeout=600)
            setfuzz.disable_active_expire(c)
            clients.append(c)
        results = {}
        far = int(time.time() * 1000) + 6 * 3600 * 1000
        for shape in shapes:
            live = [build(c, "k", a.n, shape, far) for c in clients]
            time.sleep(0.05)
            for cmd, args in CMDS:
                label = " ".join([cmd] + args)
                rounds = {n: [] for n in names}
                for _ in range(a.rounds):
                    for name, c, nlive in zip(names, clients, live):
                        key = "k"
                        reps = a.reps
                        if cmd == "SPOP":
                            c.cmd("DEL", "kp")
                            c.cmd("COPY", "k", "kp")
                            key = "kp"
                            reps = max(1, min(reps, nlive // (10 * int(args[0]))))
                        c.cmd("CONFIG", "RESETSTAT")
                        for _ in range(reps):
                            c.cmd(cmd, key, *args)
                        rounds[name].append(usec_per_call(c, cmd))
                for name in names:
                    results[(shape, label, name)] = statistics.median(rounds[name])
        print("%-10s %-16s " % ("shape", "command") + " ".join("%12s" % n for n in names))
        for shape in shapes:
            for cmd, args in CMDS:
                label = " ".join([cmd] + args)
                print("%-10s %-16s " % (shape, label) + " ".join("%12.1f" % results[(shape, label, n)] for n in names))
    finally:
        for c in clients:
            c.close()
        for s in servers:
            s.close()


if __name__ == "__main__":
    main()
