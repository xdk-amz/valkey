#!/usr/bin/env python3
"""Replica and AOF consistency after SPOP on sets with hidden members, for the
counts that reach each pop path (index answer, redraws, in-place selection pass,
take-all, whole-set CASE 1) on hashtable and listpack sets.

After every SPOP on a COPY of each shape: the replica (which never expires) must
hold exactly the primary's physical members, and an AOF reload must reproduce
the live view and SCARD. usage: repl2.py --server BIN [--port P] [--seed N]
"""
import argparse
import os
import random
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import setfuzz  # noqa: E402

FAR = setfuzz.FAR_MS


class Fail(Exception):
    pass


def build(c, key, persistent, ttl_live, hidden, prefix):
    c.cmd("DEL", key)
    p = ["%s:p%d" % (prefix, i) for i in range(persistent)]
    t = ["%s:t%d" % (prefix, i) for i in range(ttl_live)]
    h = ["%s:h%d" % (prefix, i) for i in range(hidden)]
    allm = p + t + h
    for i in range(0, len(allm), 2000):
        c.cmd("SADD", key, *allm[i:i + 2000])
    if t:
        c.cmd("SPEXPIRE", key, FAR, "MEMBERS", len(t), *t)
    if h:
        setfuzz.hide_members(c, key, h)
    return set(p + t), set(h)


def wait_sync(p, r):
    deadline = time.time() + 30
    while time.time() < deadline:
        po = int(setfuzz._field(p.cmd("INFO", "replication"), "master_repl_offset"))
        ro = int(setfuzz._field(r.cmd("INFO", "replication"), "slave_repl_offset") or -1)
        if po == ro:
            return
        time.sleep(0.01)
    raise Fail("replica did not catch up")


def run(primary, replica, seed):
    rng = random.Random(seed)
    p = primary.client(timeout=120)
    r = replica.client(timeout=120)
    setfuzz.disable_active_expire(p)
    setfuzz.disable_active_expire(r)
    p.cmd("FLUSHALL")
    p.cmd("CONFIG", "SET", "appendonly", "yes")
    setfuzz._wait_aof_ready(p)
    r.cmd("REPLICAOF", "127.0.0.1", str(primary.port))
    setfuzz._wait_sync(r)
    shapes = {
        "needle": (6, 0, 3000),
        "mixed": (60, 60, 2000),
        "allttl_few": (0, 9, 3000),
        "allttl_many": (0, 700, 2000),
        "onehidden": (300, 0, 1),
        "allhidden": (0, 0, 2500),
        "lp_mixed": (10, 10, 40),
        "lp_allhidden": (0, 0, 60),
    }
    checks = 0
    for name, (np_, nt, nh) in shapes.items():
        src = "src:" + name
        live, hidden = build(p, src, np_, nt, nh, name)
        L, size = len(live), len(live) + len(hidden)
        counts = sorted({1, 2, 3, max(1, L // 2), max(1, L - 1), L, L + 1, size, size + 10})
        for cnt in counts:
            dst = "dst:%s:%d" % (name, cnt)
            p.cmd("DEL", dst)
            p.cmd("COPY", src, dst)
            res = setfuzz.as_list(p.cmd("SPOP", dst, cnt))
            want = min(cnt, L)
            if len(res) != want or len(set(res)) != want or not set(res) <= live:
                raise Fail("%s SPOP %d returned %d members (want %d live)" % (name, cnt, len(res), want))
            left_live = live - set(res)
            physical = left_live | hidden
            if set(setfuzz.as_list(p.cmd("SMEMBERS", dst))) != left_live:
                raise Fail("%s SPOP %d: primary live view wrong" % (name, cnt))
            exists = int(p.cmd("EXISTS", dst))
            if exists != (1 if physical else 0) or (physical and int(p.cmd("SCARD", dst)) != len(physical)):
                raise Fail("%s SPOP %d: primary SCARD/EXISTS wrong" % (name, cnt))
            wait_sync(p, r)
            # A replica never removes an expired member, but hides it from
            # client reads exactly as the primary does.
            rm = set(setfuzz.as_list(r.cmd("SMEMBERS", dst)))
            rcard = int(r.cmd("SCARD", dst))
            if rm != left_live or rcard != len(physical):
                raise Fail("%s SPOP %d: replica live %d / SCARD %d, primary live %d / physical %d; missing %s extra %s" % (
                    name, cnt, len(rm), rcard, len(left_live), len(physical),
                    sorted(left_live - rm)[:3], sorted(rm - left_live)[:3]))
            checks += 1
        # SRANDMEMBER must not write anything that a replica could see
        before = int(setfuzz._field(p.cmd("INFO", "replication"), "master_repl_offset"))
        for cnt in (1, -5, 3, L, size + 10):
            p.cmd("SRANDMEMBER", src, cnt)
        p.cmd("SRANDMEMBER", src)
        after = int(setfuzz._field(p.cmd("INFO", "replication"), "master_repl_offset"))
        if after != before:
            raise Fail("%s: SRANDMEMBER changed the replication offset (%d -> %d)" % (name, before, after))
    # AOF reload reproduces the primary's state
    pre = {}
    for k in setfuzz.as_list(p.cmd("KEYS", "*")):
        pre[k] = (int(p.cmd("SCARD", k)), set(setfuzz.as_list(p.cmd("SMEMBERS", k))))
    p.cmd("DEBUG", "LOADAOF")
    for k, (card, members) in pre.items():
        if int(p.cmd("SCARD", k)) != card or set(setfuzz.as_list(p.cmd("SMEMBERS", k))) != members:
            raise Fail("AOF reload changed %s" % k)
    r.cmd("REPLICAOF", "NO", "ONE")
    p.close()
    r.close()
    return checks


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--server", required=True)
    ap.add_argument("--port", type=int, default=29000)
    ap.add_argument("--seed", type=int, default=1)
    a = ap.parse_args()
    base = os.path.join(os.environ["KIROCREW_SCRATCH"], "fuzz", "run")
    prim = setfuzz.Server(a.server, base, "repl2-p-%d" % a.port, a.port).start()
    rep = setfuzz.Server(a.server, base, "repl2-r-%d" % a.port, a.port + 1).start()
    try:
        n = run(prim, rep, a.seed)
        print("OK repl2 seed=%d checks=%d" % (a.seed, n))
        rc = 0
    except Fail as e:
        print("FAIL", e)
        rc = 1
    finally:
        rep.close()
        prim.close()
    sys.exit(rc)


if __name__ == "__main__":
    main()
