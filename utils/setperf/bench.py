#!/usr/bin/env python3
"""Latency + work-counter runner for the set member-TTL performance contracts.

Starts each given valkey-server binary on its own port, builds the same fixtures
(hashtable n members, no TTL vs one far-future TTL vs all TTLs; listpack 128),
and times a fixed list of commands. On a binary built with WORK_COUNTERS=yes it
also records DEBUG WORKCTR counters for every timed command.

    utils/setperf/bench.py --server base=/path/parent/src/valkey-server \
        --server head=/path/head/src/valkey-server --n 200000 --reps 200 \
        --out results.json

Timing is wall-clock round trip from this client (median and p99 of `reps`
runs, mutating commands re-add what they removed between runs), so use it as
supporting evidence next to the counter-based tests, not as a gate. Never run
two instances against the same ports.
"""

import argparse
import json
import os
import socket
import statistics
import subprocess
import sys
import tempfile
import time


class Client:
    def __init__(self, port):
        self.sock = socket.create_connection(("127.0.0.1", port))
        self.buf = b""

    def close(self):
        self.sock.close()

    def _read_line(self):
        while b"\r\n" not in self.buf:
            chunk = self.sock.recv(65536)
            if not chunk:
                raise IOError("connection closed")
            self.buf += chunk
        line, self.buf = self.buf.split(b"\r\n", 1)
        return line

    def _read_exact(self, n):
        while len(self.buf) < n:
            chunk = self.sock.recv(65536)
            if not chunk:
                raise IOError("connection closed")
            self.buf += chunk
        data, self.buf = self.buf[:n], self.buf[n:]
        return data

    def _read_reply(self):
        line = self._read_line()
        t, rest = line[:1], line[1:]
        if t in (b"+", b":"):
            return rest.decode()
        if t == b"-":
            raise RuntimeError(rest.decode())
        if t == b"$":
            n = int(rest)
            if n < 0:
                return None
            data = self._read_exact(n + 2)[:-2]
            return data.decode(errors="replace")
        if t in (b"*", b"%"):
            n = int(rest)
            if n < 0:
                return None
            if t == b"%":
                n *= 2
            return [self._read_reply() for _ in range(n)]
        raise RuntimeError("unexpected reply type %r" % t)

    def send(self, *args):
        parts = [b"*%d\r\n" % len(args)]
        for a in args:
            a = a if isinstance(a, bytes) else str(a).encode()
            parts.append(b"$%d\r\n%s\r\n" % (len(a), a))
        self.sock.sendall(b"".join(parts))

    def call(self, *args):
        self.send(*args)
        return self._read_reply()

    def pipeline(self, cmds):
        for c in cmds:
            self.send(*c)
        return [self._read_reply() for _ in cmds]


def start_server(binary, port, workdir):
    log = os.path.join(workdir, "server-%d.log" % port)
    proc = subprocess.Popen(
        [binary, "--port", str(port), "--save", "", "--appendonly", "no", "--dir", workdir,
         "--enable-debug-command", "yes", "--logfile", log, "--daemonize", "no"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    for _ in range(100):
        try:
            c = Client(port)
            c.call("PING")
            return proc, c
        except OSError:
            time.sleep(0.05)
    proc.kill()
    raise RuntimeError("server on port %d did not start (see %s)" % (port, log))


def members(n, prefix="m"):
    return ["%s%d" % (prefix, i) for i in range(n)]


def build_fixture(c, key, n, ttl, listpack=False):
    c.call("DEL", key)
    ms = members(n)
    for i in range(0, n, 4000):
        c.call("SADD", key, *ms[i:i + 4000])
    far = int(time.time() * 1000) + 86400 * 1000
    if ttl == "one":
        c.call("SPEXPIREAT", key, far, "MEMBERS", 1, ms[0])
    elif ttl == "one_expired":
        # one hidden member; active expiration is off for the whole run
        c.call("SPEXPIRE", key, 1, "MEMBERS", 1, ms[0])
        time.sleep(0.005)
    elif ttl == "all":
        for i in range(0, n, 4000):
            chunk = ms[i:i + 4000]
            c.call("SPEXPIRE", key, 86400000, "MEMBERS", len(chunk), *chunk)
    return ms


def has_workctr(c):
    try:
        c.call("DEBUG", "WORKCTR", "GET")
        return True
    except RuntimeError:
        return False


def timed(c, cmd, reps, restore=None):
    lat = []
    for _ in range(reps):
        t0 = time.perf_counter()
        reply = c.call(*cmd)
        lat.append((time.perf_counter() - t0) * 1e6)
        if restore:
            restore(reply)
    lat.sort()
    return {"median_us": statistics.median(lat), "p99_us": lat[int(len(lat) * 0.99) - 1], "min_us": lat[0]}


def counters(c, cmd):
    c.call("DEBUG", "WORKCTR", "SEED", 12345)
    c.call("DEBUG", "WORKCTR", "ARM")
    c.call(*cmd)
    flat = c.call("DEBUG", "WORKCTR", "GET")
    d = dict(zip(flat[0::2], [int(v) for v in flat[1::2]]))
    return {k: v for k, v in d.items() if v != 0}


def scenarios(key, n, ms):
    """(name, command, restore-kind) triples; restore keeps the fixture stable."""
    return [
        ("SPOP key", ["SPOP", key], "readd"),
        ("SPOP key 1", ["SPOP", key, 1], "readd"),
        ("SPOP key 2", ["SPOP", key, 2], "readd"),
        ("SRANDMEMBER key", ["SRANDMEMBER", key], None),
        ("SRANDMEMBER key 2", ["SRANDMEMBER", key, 2], None),
        ("SRANDMEMBER key -5", ["SRANDMEMBER", key, -5], None),
        ("SISMEMBER key m1", ["SISMEMBER", key, "m1"], None),
        ("SADD key absent", ["SADD", key, "zz-absent"], "srem-absent"),
        ("SADDEX key EX 1000 MEMBERS 1 absent", ["SADDEX", key, "EX", 1000, "MEMBERS", 1, "zz-absent2"], "srem-absent2"),
        ("MEMORY USAGE key", ["MEMORY", "USAGE", key], None),
        ("SCARD key", ["SCARD", key], None),
    ]


def run_binary(name, binary, port, n, reps, workdir, out):
    proc, c = start_server(binary, port, workdir)
    try:
        wc = has_workctr(c)
        try:
            c.call("SADD", "b:probe", "x")
            c.call("SEXPIRE", "b:probe", 1000, "MEMBERS", 1, "x")
            has_ttl = True
        except RuntimeError:
            has_ttl = False
        c.call("DEL", "b:probe")
        c.call("DEBUG", "SET-ACTIVE-EXPIRE", 0)
        out[name] = {"binary": binary, "workctr": wc, "member_ttl": has_ttl, "results": []}
        for enc, size in (("hashtable", n), ("listpack", 128)):
            for ttl in ("none", "one", "one_expired", "all"):
                if ttl != "none" and not has_ttl:
                    continue
                key = "b:%s:%s" % (enc, ttl)
                ms = build_fixture(c, key, size, ttl)
                if enc == "listpack" and c.call("OBJECT", "ENCODING", key) != "listpack":
                    continue
                for label, cmd, restore in scenarios(key, size, ms):
                    if restore == "readd":
                        def rst(reply, key=key):
                            if isinstance(reply, list):
                                if reply:
                                    c.call("SADD", key, *reply)
                            elif reply is not None:
                                c.call("SADD", key, reply)
                    elif restore and restore.startswith("srem-"):
                        member = "zz-" + restore[len("srem-"):]
                        def rst(reply, key=key, member=member):
                            c.call("SREM", key, member)
                    else:
                        rst = None
                    rec = {"encoding": enc, "n": size, "ttl": ttl, "command": label}
                    try:
                        rec.update(timed(c, cmd, reps, rst))
                    except RuntimeError as e:
                        if "unknown command" in str(e):
                            continue
                        raise
                    if wc:
                        rec["counters"] = counters(c, cmd)
                        if rst:
                            # the counted run mutated the set: rebuild the fixture
                            build_fixture(c, key, size, ttl)
                    out[name]["results"].append(rec)
                    print("%-6s %-9s %-4s %-38s median %9.1f us  p99 %9.1f us" % (
                        name, enc, ttl, label, rec["median_us"], rec["p99_us"]), file=sys.stderr)
    finally:
        try:
            c.call("SHUTDOWN", "NOSAVE")
        except Exception:
            pass
        c.close()
        proc.wait(timeout=10)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--server", action="append", required=True, metavar="NAME=PATH",
                    help="server binary to benchmark; repeat for base/head")
    ap.add_argument("--n", type=int, default=200000, help="hashtable fixture cardinality")
    ap.add_argument("--reps", type=int, default=100)
    ap.add_argument("--port", type=int, default=41000, help="first port; one per server")
    ap.add_argument("--out", default="setperf-bench.json")
    args = ap.parse_args()
    out = {"n": args.n, "reps": args.reps, "servers": {}}
    workdir = tempfile.mkdtemp(prefix="setperf-bench-", dir=os.environ.get("KIROCREW_SCRATCH") or None)
    for i, spec in enumerate(args.server):
        name, path = spec.split("=", 1)
        run_binary(name, path, args.port + i, args.n, args.reps, workdir, out["servers"])
    with open(args.out, "w") as fp:
        json.dump(out, fp, indent=1)
    print("wrote", args.out, file=sys.stderr)


if __name__ == "__main__":
    main()
