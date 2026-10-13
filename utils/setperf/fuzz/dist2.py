#!/usr/bin/env python3
"""Uniformity checks aimed at each selection step of SPOP / SRANDMEMBER on sets
with hidden (expired, unreclaimed) members. Reuses setfuzz.py's server/client.

Shapes are chosen so that a given step must serve the draws:
  needle        6 persistent live among 6000 hidden   -> redraws fail, rank/selection walk
  mixed         60 persistent + 60 TTL live, 4000 hidden -> redraws partly, then walks
  allttl_few    9 TTL live among 6000 hidden            -> index answers (step 1)
  allttl_many   700 TTL live among 3000 hidden          -> index declines (>512), redraws / walks
  onehidden     300 persistent live, 1 hidden           -> redraws (step 2)
A chi-square test per (shape, command) fails below p = 1e-6.

usage: dist2.py --server BIN [--port P] [--seed N] [--scale F]
"""
import argparse
import math
import os
import random
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import setfuzz  # noqa: E402

FAR = setfuzz.FAR_MS


def chi2_p(stat, df):
    # Wilson-Hilferty normal approximation of the chi-square upper tail.
    if df <= 0:
        return 1.0
    z = ((stat / df) ** (1.0 / 3) - (1 - 2.0 / (9 * df))) / math.sqrt(2.0 / (9 * df))
    return 0.5 * math.erfc(z / math.sqrt(2))


def build(c, key, persistent, ttl_live, hidden, ttl_groups=3):
    c.cmd("DEL", key)
    p = ["%s:p%d" % (key, i) for i in range(persistent)]
    t = ["%s:t%d" % (key, i) for i in range(ttl_live)]
    h = ["%s:h%d" % (key, i) for i in range(hidden)]
    allm = p + t + h
    for i in range(0, len(allm), 2000):
        c.cmd("SADD", key, *allm[i:i + 2000])
    # live TTLs spread over a few far deadlines (different buckets)
    for g in range(ttl_groups):
        grp = t[g::ttl_groups]
        if grp:
            c.cmd("SPEXPIRE", key, FAR + g * 60000, "MEMBERS", len(grp), *grp)
    if h:
        setfuzz.hide_members(c, key, h)
    live = set(p + t)
    enc = c.cmd("OBJECT", "ENCODING", key)
    assert enc == "hashtable", (key, enc)
    assert int(c.cmd("SCARD", key)) == len(allm)
    got = set(setfuzz.as_list(c.cmd("SMEMBERS", key)))
    assert got == live, (key, len(got), len(live))
    return live


class Fail(Exception):
    pass


def check_freq(tag, freq, live, expected_each):
    stat = 0.0
    for m in live:
        o = freq.get(m, 0)
        stat += (o - expected_each) ** 2 / expected_each
    extra = [m for m in freq if m not in live]
    if extra:
        raise Fail("%s: returned non-live members %s" % (tag, extra[:3]))
    p = chi2_p(stat, len(live) - 1)
    lo = min(freq.get(m, 0) for m in live)
    hi = max(freq.get(m, 0) for m in live)
    line = "%-38s live=%-4d exp=%-8.1f min=%-6d max=%-6d chi2=%.1f p=%.3g" % (
        tag, len(live), expected_each, lo, hi, stat, p)
    print(line, flush=True)
    if p < 1e-6:
        raise Fail("NON-UNIFORM " + line)


def run(server, seed, scale):
    random.seed(seed)
    c = server.client(timeout=120)
    setfuzz.disable_active_expire(c)
    c.cmd("FLUSHALL")
    shapes = {
        "needle": (6, 0, 6000),
        "mixed": (60, 60, 4000),
        "allttl_few": (0, 9, 6000),
        "allttl_many": (0, 700, 3000),
        "onehidden": (300, 0, 1),
    }
    for name, (np_, nt, nh) in shapes.items():
        key = "s_" + name
        live = build(c, key, np_, nt, nh)
        L = len(live)
        target = 40.0  # expected hits per member and command
        # scalar SRANDMEMBER and counted forms with replacement
        for cnt in (None, 1, -1, -5, -100):
            draws = 1 if cnt is None else abs(cnt)
            trials = max(50, int(math.ceil(target * L / draws * scale)))
            freq = {}
            for _ in range(trials):
                if cnt is None:
                    r = c.cmd("SRANDMEMBER", key)
                    res = [] if r is None else [r]
                else:
                    res = setfuzz.as_list(c.cmd("SRANDMEMBER", key, cnt))
                if len(res) != draws:
                    raise Fail("%s SRANDMEMBER %s returned %d members, want %d" % (key, cnt, len(res), draws))
                for m in res:
                    freq[m] = freq.get(m, 0) + 1
            check_freq("%s SRANDMEMBER %s" % (name, cnt), freq, live, trials * draws / L)
        # distinct counts: inclusion frequency must be count / live for every member
        for cnt in sorted({2, 3, max(2, L // 2), max(2, L - 1)}):
            if cnt > L:
                continue
            trials = max(50, int(math.ceil(target * L / cnt * scale)))
            if cnt == L:
                continue
            freq = {}
            for _ in range(trials):
                res = setfuzz.as_list(c.cmd("SRANDMEMBER", key, cnt))
                if len(res) != cnt or len(set(res)) != cnt:
                    raise Fail("%s SRANDMEMBER %d: %d members, %d distinct" % (key, cnt, len(res), len(set(res))))
                for m in res:
                    freq[m] = freq.get(m, 0) + 1
            check_freq("%s SRANDMEMBER %d" % (name, cnt), freq, live, trials * cnt / L)
        # SPOP on a copy each trial, for the pop paths
        for cnt in sorted({1, 3, max(2, L // 2), max(2, L - 1)}):
            if cnt >= L:
                continue
            trials = max(50, int(math.ceil(target * L / cnt * scale)))
            trials = min(trials, int(4000 * scale) + 50)
            freq = {}
            for _ in range(trials):
                c.cmd("DEL", "tmp")
                c.cmd("COPY", key, "tmp")
                res = setfuzz.as_list(c.cmd("SPOP", "tmp", cnt))
                if len(res) != cnt or len(set(res)) != cnt:
                    raise Fail("%s SPOP %d: %d members" % (key, cnt, len(res)))
                left = set(setfuzz.as_list(c.cmd("SMEMBERS", "tmp")))
                if left & set(res) or len(left) != L - cnt:
                    raise Fail("%s SPOP %d did not remove exactly what it returned" % (key, cnt))
                for m in res:
                    freq[m] = freq.get(m, 0) + 1
            check_freq("%s SPOP %d" % (name, cnt), freq, live, trials * cnt / L)
    c.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--server", required=True)
    ap.add_argument("--port", type=int, default=26000)
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--scale", type=float, default=1.0)
    a = ap.parse_args()
    base = os.path.join(os.environ.get("KIROCREW_SCRATCH", "."), "fuzz", "run")
    s = setfuzz.Server(a.server, base, "dist2-%d" % a.port, a.port).start()
    t0 = time.time()
    try:
        run(s, a.seed, a.scale)
        print("OK dist2 seed=%d %.1fs" % (a.seed, time.time() - t0))
        rc = 0
    except Fail as e:
        print("FAIL", e)
        rc = 1
    finally:
        s.close()
    sys.exit(rc)


if __name__ == "__main__":
    main()
