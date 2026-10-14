#!/usr/bin/env python3
"""setfuzz.py -- black-box MODEL FUZZER for Valkey SET member TTLs.

It drives a real valkey-server over a socket and checks the member-TTL
invariants against an in-process model of every member's category --
persistent, live (a far-future TTL whose exact deadline the model knows) or
hidden (expired but not yet reclaimed, with active expiration disabled):

  * SCARD counts every physical member (live + hidden);
  * SMEMBERS / SISMEMBER expose only live members;
  * SPEXPIRETIME reports -1 for a persistent member, the exact deadline for a
    live one, and -2 for a hidden or absent one;
  * SPOP removes only live members and never a hidden one, the count is right
    and the members are distinct;
  * SRANDMEMBER returns only live members (distinct for a positive count, with
    repetition allowed for a negative count) and never mutates the set;
  * SADD and SADDEX count a hidden member as added and replace it in place;
    SADD leaves an existing member's TTL alone, SADDEX sets it;
  * SPERSIST and SPEXPIREAT (plain, NX, XX) reply exactly per member and treat
    a hidden member as absent (-2) without reviving it;
  * SREM counts and removes only live members, leaving hidden ones in place;
  * active expiration reclaims every hidden member and nothing else;
  * an RDB round trip (DEBUG RELOAD) reclaims the hidden members while an AOF
    replay (DEBUG LOADAOF) preserves them -- the two documented load paths --
    and both keep every live deadline.

The operations grow the set as well as shrink it, so a run crosses the
intset/listpack/hashtable conversions, moves members between expiry-index
buckets and keeps going for as many operations as asked. Live deadlines are
absolute and hours away, and a member is made hidden by giving it a 1 ms TTL
and waiting, so a member never changes category underneath a check. The server
picks the random members; the model follows its choice and validates the
constraints around it, so a seed fixes the fuzzer's own choices but not the
server's.

This module is also imported by the sibling scripts (dist2.py uniformity,
repl2.py replication/AOF, bench.py latency), which reuse its Server/client and
helpers, so the names below are a stable surface.

usage: setfuzz.py --server BIN [--port P] [--seed N] [--seeds N] [--ops N]
                  [--mode model|persist|repl|selftest|all] [--debug-assert]
"""
import argparse
import os
import random
import socket
import subprocess
import sys
import time

EXPIRY_NONE = -1
# A relative TTL (ms) far enough out that a "live" member never expires during a
# run. Used with SPEXPIRE (relative) by the sibling scripts.
FAR_MS = 3600 * 1000


# --------------------------------------------------------------------------- #
# RESP2 client
# --------------------------------------------------------------------------- #
class ProtocolError(Exception):
    pass


class ServerError(Exception):
    pass


class Conn:
    """A minimal synchronous RESP2 client."""

    def __init__(self, port, host="127.0.0.1", timeout=60):
        self.sock = socket.create_connection((host, port), timeout=timeout)
        self.sock.settimeout(timeout)
        self.buf = b""

    def _read_line(self):
        while b"\r\n" not in self.buf:
            chunk = self.sock.recv(65536)
            if not chunk:
                raise ProtocolError("connection closed by server")
            self.buf += chunk
        line, self.buf = self.buf.split(b"\r\n", 1)
        return line

    def _read_n(self, n):
        while len(self.buf) < n + 2:
            chunk = self.sock.recv(65536)
            if not chunk:
                raise ProtocolError("connection closed by server")
            self.buf += chunk
        data, self.buf = self.buf[:n], self.buf[n + 2:]
        return data

    def _read_reply(self):
        line = self._read_line()
        t, rest = line[:1], line[1:]
        if t == b"+":
            return rest.decode("utf-8", "replace")
        if t == b"-":
            raise ServerError(rest.decode("utf-8", "replace"))
        if t == b":":
            return int(rest)
        if t == b"$":
            n = int(rest)
            if n < 0:
                return None
            return self._read_n(n).decode("utf-8", "replace")
        if t == b"*":
            n = int(rest)
            if n < 0:
                return None
            return [self._read_reply() for _ in range(n)]
        raise ProtocolError("unexpected reply byte %r" % t)

    def cmd(self, *args):
        out = [b"*%d\r\n" % len(args)]
        for a in args:
            b = a if isinstance(a, bytes) else str(a).encode("utf-8")
            out.append(b"$%d\r\n" % len(b))
            out.append(b)
            out.append(b"\r\n")
        self.sock.sendall(b"".join(out))
        return self._read_reply()

    def close(self):
        try:
            self.sock.close()
        except OSError:
            pass


# --------------------------------------------------------------------------- #
# Server lifecycle
# --------------------------------------------------------------------------- #
class Server:
    """Spawns a disposable valkey-server under a scratch dir and talks to it."""

    def __init__(self, server_path, base_dir, name, port, extra=None):
        self.server_path = server_path
        self.port = port
        self.name = name
        self.dir = os.path.join(base_dir, name)
        self.extra = list(extra or [])
        self.proc = None
        self.conn = None

    def start(self):
        os.makedirs(self.dir, exist_ok=True)
        args = [
            self.server_path,
            "--port", str(self.port),
            "--bind", "127.0.0.1",
            "--dir", self.dir,
            "--save", "",
            "--appendonly", "no",
            "--daemonize", "no",
            "--enable-debug-command", "yes",
            "--logfile", os.path.join(self.dir, "server.log"),
        ] + self.extra
        self.proc = subprocess.Popen(args, stdout=subprocess.DEVNULL,
                                     stderr=subprocess.DEVNULL)
        deadline = time.time() + 30
        while time.time() < deadline:
            if self.proc.poll() is not None:
                raise ProtocolError("%s exited early (rc=%s); see %s/server.log"
                                    % (self.name, self.proc.returncode, self.dir))
            try:
                c = Conn(self.port, timeout=5)
                if c.cmd("PING") == "PONG":
                    self.conn = c
                    return self
                c.close()
            except (OSError, ProtocolError):
                time.sleep(0.05)
        raise ProtocolError("%s did not become ready" % self.name)

    def client(self, timeout=60):
        return Conn(self.port, timeout=timeout)

    def cmd(self, *args):
        if self.conn is None:
            self.conn = self.client()
        return self.conn.cmd(*args)

    def close(self):
        if self.proc is None:
            return
        try:
            c = self.client(timeout=5)
            c.cmd("SHUTDOWN", "NOSAVE")
        except (OSError, ProtocolError, ServerError):
            pass
        try:
            self.proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait()
        if self.conn:
            self.conn.close()


# --------------------------------------------------------------------------- #
# Helpers shared with the sibling scripts
# --------------------------------------------------------------------------- #
def as_list(reply):
    """Normalise any SMEMBERS/SPOP/SRANDMEMBER reply to a list of str."""
    if reply is None:
        return []
    if isinstance(reply, (list, tuple)):
        return [x if isinstance(x, str) else str(x) for x in reply]
    return [reply if isinstance(reply, str) else str(reply)]


def disable_active_expire(c):
    c.cmd("DEBUG", "SET-ACTIVE-EXPIRE", 0)


def _field(info_text, name):
    """Return the value of an INFO field, or None."""
    for line in info_text.splitlines():
        if line.startswith(name + ":"):
            return line.split(":", 1)[1].strip()
    return None


def hide_members(c, key, members):
    """Make `members` hidden: give them a 1 ms TTL and wait it out. The caller
    must have disabled active expiration, so they stay physically present."""
    members = list(members)
    for i in range(0, len(members), 2000):
        chunk = members[i:i + 2000]
        c.cmd("SPEXPIRE", key, 1, "MEMBERS", len(chunk), *chunk)
    time.sleep(0.02)


def _wait_aof_ready(c, timeout=30):
    deadline = time.time() + timeout
    while time.time() < deadline:
        info = c.cmd("INFO", "persistence")
        if _field(info, "aof_rewrite_in_progress") == "0" and \
                _field(info, "aof_last_bgrewrite_status") == "ok":
            return
        time.sleep(0.02)
    raise ProtocolError("AOF did not become ready")


def _wait_sync(r, timeout=30):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if _field(r.cmd("INFO", "replication"), "master_link_status") == "up":
            return
        time.sleep(0.02)
    raise ProtocolError("replica did not sync")


# --------------------------------------------------------------------------- #
# Model
# --------------------------------------------------------------------------- #
# Member categories. NEW and ABSENT only label the members picked for one
# command: a fresh name, and a name that is never in the set.
PERSIST, LIVE_TTL, HIDDEN = "P", "T", "H"
NEW, ABSENT = "N", "A"

# Live deadlines are absolute, hours away, and spread so the expiry index sees
# shared deadlines, neighbours inside one bucket window (8192 ms) and separate
# buckets.
DEADLINE_OFFSETS_MS = (0, 0, 1, 7, 4096, 8191, 8192, 9000, 16384, 65536, 600000, 3600000)

# The operations of the model mode and their weights. The set is kept under
# MAX_PHYSICAL members so the per-operation SMEMBERS check stays cheap.
OPS = (("spop", 3), ("srand", 2), ("sadd", 3), ("saddex", 3), ("srem", 2),
       ("hide", 2), ("persist", 2), ("retime", 2), ("reclaim", 0.15))
MAX_PHYSICAL = 2000

# Picks which members the per-operation checks sample; independent of --seed.
_sample_rng = random.Random(0)


class Checker(Exception):
    pass


def _live(cat):
    return cat in (PERSIST, LIVE_TTL)


def deadline_pool():
    base = int(time.time() * 1000) + 4 * 3600 * 1000
    return [base + off for off in DEADLINE_OFFSETS_MS]


class SetModel:
    """Tracks the intended category of every member of one set, and the
    absolute deadline of every live member that has a TTL."""

    def __init__(self):
        self.m = {}          # member -> category
        self.at = {}         # LIVE_TTL member -> absolute deadline (ms)
        self._seq = 0

    def fresh(self, n, prefix, numeric=False):
        """`n` names never used before; integers when `numeric`, so a set of
        them is an intset."""
        out = []
        for _ in range(n):
            out.append(str(10 ** 6 + self._seq) if numeric else "%s%d" % (prefix, self._seq))
            self._seq += 1
        return out

    def put(self, mem, cat, at=None):
        self.m[mem] = cat
        if cat == LIVE_TTL:
            self.at[mem] = at
        else:
            self.at.pop(mem, None)

    def drop(self, mem):
        self.m.pop(mem, None)
        self.at.pop(mem, None)

    def clear(self):
        self.m.clear()
        self.at.clear()

    def live(self):
        return {k for k, v in self.m.items() if _live(v)}

    def hidden(self):
        return {k for k, v in self.m.items() if v == HIDDEN}

    def physical(self):
        return set(self.m)


def check_views(c, key, model, where):
    """Validate SCARD / SMEMBERS / EXISTS / SISMEMBER / SPEXPIRETIME against the
    model (active expiry off, so hidden members are physically present but not
    exposed)."""
    live = model.live()
    physical = model.physical()

    card = int(c.cmd("SCARD", key))
    if card != len(physical):
        raise Checker("%s: SCARD=%d, model physical=%d (%s)"
                      % (key, card, len(physical), where))

    got = set(as_list(c.cmd("SMEMBERS", key)))
    if got != live:
        raise Checker("%s: SMEMBERS live mismatch (%s); missing=%s extra=%s"
                      % (key, where, sorted(live - got)[:4], sorted(got - live)[:4]))

    exists = int(c.cmd("EXISTS", key))
    if exists != (1 if physical else 0):
        raise Checker("%s: EXISTS=%d physical=%d (%s)"
                      % (key, exists, len(physical), where))

    sample = list(live)[:3] + list(model.hidden())[:3] + ["__absent__"]
    for mem in sample:
        want = 1 if mem in live else 0
        if int(c.cmd("SISMEMBER", key, mem)) != want:
            raise Checker("%s: SISMEMBER %s != %d (%s)" % (key, mem, want, where))

    # TTL state, exactly. Checked last, so the selftest's faults planted in the
    # views above still fire where they are planted.
    persist = sorted(k for k, v in model.m.items() if v == PERSIST)
    ttl = sorted(model.at)
    hid = sorted(model.hidden())
    probe = (_sample_rng.sample(persist, min(2, len(persist)))
             + _sample_rng.sample(ttl, min(3, len(ttl)))
             + _sample_rng.sample(hid, min(2, len(hid))) + ["__absent__"])
    want = [-1 if model.m.get(k) == PERSIST else model.at.get(k, -2) for k in probe]
    got = c.cmd("SPEXPIRETIME", key, "MEMBERS", len(probe), *probe)
    if got != want:
        raise Checker("%s: SPEXPIRETIME %s -> %s, want %s (%s)"
                      % (key, probe, got, want, where))


def check_srandmember(c, key, model, where):
    """SRANDMEMBER never returns a hidden member, honours its count sign, and
    never mutates the set."""
    live = model.live()
    card_before = int(c.cmd("SCARD", key))

    one = as_list(c.cmd("SRANDMEMBER", key))
    if live:
        if len(one) != 1 or one[0] not in live:
            raise Checker("%s: SRANDMEMBER scalar=%r not a live member (%s)" % (key, one, where))
    elif one:
        raise Checker("%s: SRANDMEMBER scalar returned %r on an all-hidden set (%s)" % (key, one, where))

    n = len(live)
    for cnt in (1, max(1, n // 2), n, n + 5):
        got = as_list(c.cmd("SRANDMEMBER", key, cnt))
        want = min(cnt, n)
        if len(got) != want or len(set(got)) != want or not set(got) <= live:
            raise Checker("%s: SRANDMEMBER %d -> %d members (want %d distinct live) (%s)"
                          % (key, cnt, len(got), want, where))
    for cnt in (1, max(1, n // 2), n + 5):
        got = as_list(c.cmd("SRANDMEMBER", key, -cnt))
        if len(got) != (cnt if n else 0) or not set(got) <= live:
            raise Checker("%s: SRANDMEMBER -%d -> %d members (want %d live, repeats ok) (%s)"
                          % (key, cnt, len(got), cnt if n else 0, where))

    if int(c.cmd("SCARD", key)) != card_before:
        raise Checker("%s: SRANDMEMBER mutated the set (%s)" % (key, where))


def do_spop(c, key, model, cnt, where):
    """SPOP `cnt` and reconcile the model with what left."""
    live = model.live()
    got = as_list(c.cmd("SPOP", key, cnt))
    want = min(cnt, len(live))
    if len(got) != want or len(set(got)) != want:
        raise Checker("%s: SPOP %d -> %d members (want %d distinct) (%s)"
                      % (key, cnt, len(got), want, where))
    bad = set(got) - live
    if bad:
        raise Checker("%s: SPOP %d returned non-live members %s (%s)"
                      % (key, cnt, sorted(bad)[:4], where))
    for mem in got:
        model.drop(mem)
    check_views(c, key, model, where + "/post-spop")


# --------------------------------------------------------------------------- #
# Fuzzer modes
# --------------------------------------------------------------------------- #
def build_shape(c, model, key, persist, live_ttl, hidden, deadlines=None, numeric=False):
    """Populate `key` with the given category counts: far-future absolute
    deadlines for the live-TTL members and expired TTLs for the hidden ones.
    With `numeric` every name is an integer, so the set starts as an intset."""
    c.cmd("DEL", key)
    model.clear()
    deadlines = deadlines or deadline_pool()
    p = model.fresh(persist, key + ":p", numeric)
    t = model.fresh(live_ttl, key + ":t", numeric)
    h = model.fresh(hidden, key + ":h", numeric)
    allm = p + t + h
    for i in range(0, len(allm), 2000):
        c.cmd("SADD", key, *allm[i:i + 2000])
    for mem in allm:
        model.put(mem, PERSIST)
    # Live TTLs: a few far-future deadlines, two of them inside one index
    # bucket window and the others in separate buckets.
    groups = min(4, max(1, len(t)))
    for g in range(groups):
        grp = t[g::groups]
        if grp:
            at = deadlines[(3 * g) % len(deadlines)]
            c.cmd("SPEXPIREAT", key, at, "MEMBERS", len(grp), *grp)
            for mem in grp:
                model.put(mem, LIVE_TTL, at)
    if h:
        hide_members(c, key, h)
        for mem in h:
            model.put(mem, HIDDEN)


def pick(rng, model, key, new=0, persist=0, ttl=0, hidden=0, absent=0, numeric=False):
    """Distinct members for one command, as [(member, category)] in random
    order. Asking for more of a category than exist takes what there is."""
    pools = {PERSIST: [], LIVE_TTL: [], HIDDEN: []}
    for mem, cat in model.m.items():
        pools[cat].append(mem)
    out = []
    for cat, n in ((PERSIST, persist), (LIVE_TTL, ttl), (HIDDEN, hidden)):
        for mem in rng.sample(pools[cat], min(n, len(pools[cat]))):
            out.append((mem, cat))
    out += [(mem, NEW) for mem in model.fresh(new, key + ":n", numeric)]
    out += [("__absent%d__" % i, ABSENT) for i in range(absent)]
    rng.shuffle(out)
    return out


def _names(picks):
    return [mem for mem, _ in picks]


def do_sadd(c, key, model, picks, where):
    """SADD: new and hidden members count as added (a hidden one is replaced in
    place); an existing live member keeps its TTL."""
    want = sum(1 for _, cat in picks if cat in (NEW, HIDDEN))
    got = int(c.cmd("SADD", key, *_names(picks)))
    if got != want:
        raise Checker("%s: SADD %s -> %d, want %d (%s)" % (key, picks, got, want, where))
    for mem, cat in picks:
        if cat in (NEW, HIDDEN):
            model.put(mem, PERSIST)
    check_views(c, key, model, where)


def do_saddex(c, key, model, picks, at, where):
    """SADDEX PXAT: every member named ends up live with that deadline."""
    names = _names(picks)
    got = c.cmd("SADDEX", key, "PXAT", at, "MEMBERS", len(names), *names)
    if got != 1:
        raise Checker("%s: SADDEX PXAT %d %s -> %r, want 1 (%s)" % (key, at, picks, got, where))
    for mem in names:
        model.put(mem, LIVE_TTL, at)
    check_views(c, key, model, where)


def do_persist(c, key, model, picks, where):
    """SPERSIST: 1 for a live member with a TTL, -1 for a persistent one, -2 for
    a hidden or absent one, which stays as it was."""
    names = _names(picks)
    want = [1 if cat == LIVE_TTL else -1 if cat == PERSIST else -2 for _, cat in picks]
    got = c.cmd("SPERSIST", key, "MEMBERS", len(names), *names)
    if got != want:
        raise Checker("%s: SPERSIST %s -> %s, want %s (%s)" % (key, picks, got, want, where))
    for mem, cat in picks:
        if cat == LIVE_TTL:
            model.put(mem, PERSIST)
    check_views(c, key, model, where)


def do_retime(c, key, model, picks, at, cond, where):
    """SPEXPIREAT a far deadline, optionally NX (only members without a TTL) or
    XX (only members with one); hidden and absent members reply -2."""
    def outcome(cat):
        if not _live(cat):
            return -2
        if (cond == "NX" and cat == LIVE_TTL) or (cond == "XX" and cat == PERSIST):
            return 0
        return 1

    names = _names(picks)
    want = [outcome(cat) for _, cat in picks]
    got = c.cmd("SPEXPIREAT", key, at, *([cond] if cond else []), "MEMBERS", len(names), *names)
    if got != want:
        raise Checker("%s: SPEXPIREAT %d %s %s -> %s, want %s (%s)"
                      % (key, at, cond or "", picks, got, want, where))
    for (mem, _), w in zip(picks, want):
        if w == 1:
            model.put(mem, LIVE_TTL, at)
    check_views(c, key, model, where)


def do_srem(c, key, model, picks, where):
    """SREM counts and removes only live members; hidden ones stay in place."""
    want = sum(1 for _, cat in picks if _live(cat))
    got = int(c.cmd("SREM", key, *_names(picks)))
    if got != want:
        raise Checker("%s: SREM %s -> %d, want %d (%s)" % (key, picks, got, want, where))
    for mem, cat in picks:
        if _live(cat):
            model.drop(mem)
    check_views(c, key, model, where)


def do_hide(c, key, model, victims, where):
    hide_members(c, key, victims)
    for mem in victims:
        model.put(mem, HIDDEN)
    check_views(c, key, model, where)


def do_reclaim(c, key, model, where):
    """Let active expiration reclaim every hidden member, and nothing else,
    then switch it off again."""
    want = len(model.live())
    c.cmd("DEBUG", "SET-ACTIVE-EXPIRE", 1)
    try:
        deadline = time.time() + 30
        while True:
            card = int(c.cmd("SCARD", key))
            if card == want:
                break
            if card < want or time.time() > deadline:
                raise Checker("%s: active expiration left SCARD=%d, want the %d live members (%s)"
                              % (key, card, want, where))
            time.sleep(0.01)
    finally:
        disable_active_expire(c)
    for mem in list(model.hidden()):
        model.drop(mem)
    check_views(c, key, model, where)


def choose_op(rng, model):
    phys, live = len(model.m), len(model.live())
    if phys >= MAX_PHYSICAL:
        if not live:
            return "reclaim"
        allowed = ("spop", "srem", "srand", "reclaim")
    elif not live:
        allowed = ("sadd", "saddex", "srand", "spop", "persist", "retime", "reclaim")
    else:
        allowed = None
    names, weights = zip(*[(n, w) for n, w in OPS if allowed is None or n in allowed])
    return rng.choices(names, weights)[0]


def run_op(c, key, model, rng, op, deadlines, numeric):
    live = model.live()
    if op == "spop":
        n, phys = len(live), len(model.m)
        do_spop(c, key, model,
                rng.choice([1, 2, 5, max(1, n // 2), n + 3, max(1, phys), phys + 1]), "op-spop")
    elif op == "srand":
        check_srandmember(c, key, model, "op-srand")
    elif op == "sadd":
        picks = pick(rng, model, key, new=rng.randint(0, 3), persist=rng.randint(0, 1),
                     ttl=rng.randint(0, 1), hidden=rng.randint(0, 2),
                     numeric=numeric and rng.random() < 0.8)
        do_sadd(c, key, model, picks or pick(rng, model, key, new=1, numeric=numeric), "op-sadd")
    elif op == "saddex":
        picks = pick(rng, model, key, new=rng.randint(0, 3), persist=rng.randint(0, 2),
                     ttl=rng.randint(0, 2), hidden=rng.randint(0, 2), numeric=numeric)
        do_saddex(c, key, model, picks or pick(rng, model, key, new=1, numeric=numeric),
                  rng.choice(deadlines), "op-saddex")
    elif op == "srem":
        picks = pick(rng, model, key, persist=rng.randint(0, 2), ttl=rng.randint(0, 2),
                     hidden=rng.randint(0, 2), absent=rng.randint(0, 1))
        do_srem(c, key, model, picks or [("__absent0__", ABSENT)], "op-srem")
    elif op == "hide":
        victims = rng.sample(sorted(live), min(len(live), rng.choice([1, 5])))
        if victims:
            do_hide(c, key, model, victims, "op-hide")
    elif op == "persist":
        picks = pick(rng, model, key, persist=rng.randint(0, 1), ttl=rng.randint(0, 3),
                     hidden=rng.randint(0, 1), absent=rng.randint(0, 1))
        do_persist(c, key, model, picks or [("__absent0__", ABSENT)], "op-persist")
    elif op == "retime":
        picks = pick(rng, model, key, persist=rng.randint(0, 2), ttl=rng.randint(0, 2),
                     hidden=rng.randint(0, 1), absent=rng.randint(0, 1))
        do_retime(c, key, model, picks or [("__absent0__", ABSENT)], rng.choice(deadlines),
                  rng.choice([None, None, "NX", "XX"]), "op-retime")
    elif op == "reclaim":
        do_reclaim(c, key, model, "op-reclaim")
    else:
        raise ValueError(op)


def mode_model(server, seed, ops):
    rng = random.Random(seed)
    c = server.client(timeout=120)
    disable_active_expire(c)
    c.cmd("FLUSHALL")
    key = "mdl"
    model = SetModel()
    deadlines = deadline_pool()
    # Starting shapes (persistent, live-TTL, hidden, integer names): listpack
    # and hashtable, one and several index buckets, all-TTL with few live
    # members (answered from the index), all hidden, and an intset.
    shapes = [(6, 0, 40, False), (40, 40, 400, False), (0, 9, 600, False),
              (0, 300, 1500, False), (300, 0, 1, False), (0, 0, 500, False),
              (5, 5, 5, False), (50, 0, 0, True)]
    checks = 0
    for persist, live_ttl, hidden, numeric in shapes:
        where = "build%s" % ((persist, live_ttl, hidden, numeric),)
        build_shape(c, model, key, persist, live_ttl, hidden, deadlines, numeric)
        check_views(c, key, model, where)
        check_srandmember(c, key, model, where)
        checks += 1
        for _ in range(ops):
            run_op(c, key, model, rng, choose_op(rng, model), deadlines, numeric)
            checks += 1
    c.close()
    return checks


def mode_persist(server, seed):
    """RDB (DEBUG RELOAD) reclaims hidden members; AOF (DEBUG LOADAOF) keeps
    them."""
    c = server.client(timeout=120)
    disable_active_expire(c)
    c.cmd("FLUSHALL")
    model = SetModel()
    checks = 0
    for shape in [(40, 40, 400), (0, 9, 600), (0, 0, 300), (6, 0, 1)]:
        # ---- RDB round trip: hidden members are dropped ----
        build_shape(c, model, "rdb", *shape)
        check_views(c, "rdb", model, "rdb/pre")
        c.cmd("DEBUG", "RELOAD")
        for mem in list(model.hidden()):
            model.drop(mem)
        check_views(c, "rdb", model, "rdb/post-reload")
        checks += 1

        # ---- AOF replay: hidden members survive ----
        c.cmd("CONFIG", "SET", "appendonly", "yes")
        _wait_aof_ready(c)
        build_shape(c, model, "aof", *shape)
        check_views(c, "aof", model, "aof/pre")
        c.cmd("DEBUG", "LOADAOF")
        check_views(c, "aof", model, "aof/post-loadaof")
        c.cmd("CONFIG", "SET", "appendonly", "no")
        checks += 1
    c.close()
    return checks


def _clone_model(src):
    m = SetModel()
    m.m = dict(src.m)
    m.at = dict(src.at)
    return m


SELFTEST_CASES = 10


def mode_selftest():
    """Prove the invariant checkers actually fire, by driving them against
    deliberately wrong replies through a fake connection. No server needed."""
    planted = 0

    class FakeConn:
        def __init__(self, replies):
            self.replies = replies

        def cmd(self, *args):
            r = self.replies.get(args[0].upper())
            return r(args) if callable(r) else r

    model = SetModel()
    model.m = {"a": PERSIST, "b": LIVE_TTL, "c": HIDDEN}  # live {a,b}, physical {a,b,c}
    model.at = {"b": 4102444800000}
    good_views = {"SCARD": 3, "SMEMBERS": ["a", "b"], "EXISTS": 1,
                  "SISMEMBER": lambda a: 1 if a[2] in ("a", "b") else 0,
                  "SPEXPIRETIME": lambda a: [{"a": -1, "b": 4102444800000}.get(m, -2)
                                             for m in a[4:]]}

    def expect_fire(desc, fn):
        nonlocal planted
        try:
            fn()
        except Checker:
            planted += 1
            return
        raise SystemExit("SELFTEST FAILED: checker did not fire for %s" % desc)

    # Control: correct replies pass, so a fault planted on top of them is what
    # makes a check below fire.
    try:
        check_views(FakeConn(good_views), "k", model, "selftest-control")
    except Checker as e:
        raise SystemExit("SELFTEST FAILED: correct replies were rejected: %s" % e)

    # 1. SMEMBERS leaks a hidden member.
    expect_fire("SMEMBERS leaks hidden", lambda: check_views(
        FakeConn({"SCARD": 3, "SMEMBERS": ["a", "b", "c"], "EXISTS": 1,
                  "SISMEMBER": lambda a: 1}), "k", model, "selftest"))
    # 2. SCARD omits a hidden (physical) member.
    expect_fire("SCARD undercounts physical", lambda: check_views(
        FakeConn({"SCARD": 2, "SMEMBERS": ["a", "b"], "EXISTS": 1,
                  "SISMEMBER": lambda a: 1 if a[2] in ("a", "b") else 0}), "k", model, "selftest"))
    # 3. SISMEMBER true for a hidden member.
    expect_fire("SISMEMBER true on hidden", lambda: check_views(
        FakeConn({"SCARD": 3, "SMEMBERS": ["a", "b"], "EXISTS": 1,
                  "SISMEMBER": lambda a: 1}), "k", model, "selftest"))
    # 4. SRANDMEMBER returns a hidden member.
    expect_fire("SRANDMEMBER returns hidden", lambda: check_srandmember(
        FakeConn({"SCARD": 3, "SRANDMEMBER": lambda a: "c" if len(a) == 2 else ["c"]}),
        "k", model, "selftest"))
    # 5. SRANDMEMBER positive count returns duplicates.
    expect_fire("SRANDMEMBER dup on positive count", lambda: check_srandmember(
        FakeConn({"SCARD": 3, "SRANDMEMBER": lambda a: "a" if len(a) == 2 else ["a", "a"]}),
        "k", model, "selftest"))
    # 6. SPOP returns a hidden member.
    expect_fire("SPOP returns hidden", lambda: do_spop(
        FakeConn({"SPOP": ["c"]}), "k", _clone_model(model), 1, "selftest"))
    # 7. SPOP returns the wrong count.
    expect_fire("SPOP wrong count", lambda: do_spop(
        FakeConn({"SPOP": ["a", "b"]}), "k", _clone_model(model), 1, "selftest"))
    # 8. SPERSIST acts on a hidden member.
    expect_fire("SPERSIST acts on hidden", lambda: do_persist(
        FakeConn({"SPERSIST": [1]}), "k", _clone_model(model), [("c", HIDDEN)], "selftest"))
    # 9. SREM counts a hidden member as removed.
    expect_fire("SREM counts hidden", lambda: do_srem(
        FakeConn({"SREM": 2}), "k", _clone_model(model), [("a", PERSIST), ("c", HIDDEN)],
        "selftest"))
    # 10. SPEXPIRETIME loses a live member's deadline.
    expect_fire("SPEXPIRETIME loses a deadline", lambda: check_views(
        FakeConn(dict(good_views, SPEXPIRETIME=lambda a: [-1] * int(a[3]))), "k", model,
        "selftest"))

    print("selftest: %d/%d invariant checkers fired as expected" % (planted, SELFTEST_CASES))
    return planted


# --------------------------------------------------------------------------- #
def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--server", help="path to valkey-server")
    ap.add_argument("--port", type=int, default=27300)
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--seeds", type=int, default=1,
                    help="run the model mode for seeds SEED .. SEED+SEEDS-1 on one server")
    ap.add_argument("--ops", type=int, default=200, help="operations per starting shape")
    ap.add_argument("--mode", default="all",
                    choices=["model", "persist", "repl", "selftest", "all"])
    ap.add_argument("--debug-assert", action="store_true",
                    help="start the server with --enable-debug-assert yes")
    a = ap.parse_args()

    if a.mode == "selftest":
        sys.exit(0 if mode_selftest() == SELFTEST_CASES else 1)

    if not a.server:
        ap.error("--server is required for mode %s" % a.mode)

    # The selftest always runs first (it needs no server and guards the rest).
    if a.mode == "all" and mode_selftest() != SELFTEST_CASES:
        sys.exit(1)

    base = os.path.join(os.environ.get("KIROCREW_SCRATCH", "/tmp"), "fuzz", "run")
    extra = ["--enable-debug-assert", "yes"] if a.debug_assert else None
    s = Server(a.server, base, "setfuzz-%d" % a.port, a.port, extra=extra).start()
    t0 = time.time()
    seed = a.seed
    try:
        total = 0
        if a.mode in ("model", "all"):
            for seed in range(a.seed, a.seed + a.seeds):
                total += mode_model(s, seed, a.ops)
        if a.mode in ("persist", "all"):
            total += mode_persist(s, a.seed)
        if a.mode == "repl":
            print("repl mode lives in repl2.py (needs two servers)", file=sys.stderr)
        print("OK setfuzz mode=%s seeds=%d..%d ops=%d checks=%d %.1fs"
              % (a.mode, a.seed, a.seed + a.seeds - 1, a.ops, total, time.time() - t0))
        rc = 0
    except (Checker, ProtocolError, ServerError) as e:
        print("FAIL setfuzz mode=%s seed=%d: %s (server log: %s/server.log)"
              % (a.mode, seed, e, s.dir))
        rc = 1
    finally:
        s.close()
    sys.exit(rc)


if __name__ == "__main__":
    main()
