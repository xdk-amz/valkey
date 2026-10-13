#!/usr/bin/env python3
"""setfuzz.py -- black-box MODEL FUZZER for Valkey SET member TTLs.

It drives a real valkey-server over a socket and checks the member-TTL
invariants against an in-process model of which members are persistent, live
(a far-future TTL) or hidden (expired but not yet reclaimed, with active
expiration disabled):

  * SCARD counts every physical member (live + hidden);
  * SMEMBERS / SISMEMBER expose only live members;
  * SPOP removes only live members and never a hidden one, the count is right
    and the members are distinct;
  * SRANDMEMBER returns only live members (distinct for a positive count, with
    repetition allowed for a negative count) and never mutates the set;
  * SREM removes exactly what it is given;
  * an RDB round trip (DEBUG RELOAD) reclaims the hidden members while an AOF
    replay (DEBUG LOADAOF) preserves them -- the two documented load paths.

The model tracks hidden-ness explicitly (a member is made hidden by giving it a
1 ms TTL and waiting) and only ever uses far-future TTLs for live members, so a
member never changes category underneath a check and the run is deterministic
for a given seed. The server picks the random members; the model follows the
server's choice and validates the constraints around it.

This module is also imported by the sibling scripts (dist2.py uniformity,
repl2.py replication/AOF, bench.py latency), which reuse its Server/client and
helpers, so the names below are a stable surface.

usage: setfuzz.py --server BIN [--port P] [--seed N] [--ops N]
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
# Member categories.
PERSIST, LIVE_TTL, HIDDEN = "P", "T", "H"


class Checker(Exception):
    pass


def _live(cat):
    return cat in (PERSIST, LIVE_TTL)


class SetModel:
    """Tracks the intended category of every member of one set."""

    def __init__(self):
        self.m = {}          # member -> category
        self._seq = 0

    def fresh(self, n, prefix):
        out = []
        for _ in range(n):
            out.append("%s%d" % (prefix, self._seq))
            self._seq += 1
        return out

    def live(self):
        return {k for k, v in self.m.items() if _live(v)}

    def hidden(self):
        return {k for k, v in self.m.items() if v == HIDDEN}

    def physical(self):
        return set(self.m)


def check_views(c, key, model, where):
    """Validate SCARD / SMEMBERS / EXISTS / SISMEMBER against the model (active
    expiry off, so hidden members are physically present but not exposed)."""
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
        del model.m[mem]
    check_views(c, key, model, where + "/post-spop")


# --------------------------------------------------------------------------- #
# Fuzzer modes
# --------------------------------------------------------------------------- #
def build_shape(c, model, key, persist, live_ttl, hidden):
    """Populate `key` with the given category counts: far-future TTLs for the
    live-TTL members and expired TTLs for the hidden ones."""
    c.cmd("DEL", key)
    model.m.clear()
    p = model.fresh(persist, key + ":p")
    t = model.fresh(live_ttl, key + ":t")
    h = model.fresh(hidden, key + ":h")
    allm = p + t + h
    for i in range(0, len(allm), 2000):
        c.cmd("SADD", key, *allm[i:i + 2000])
    # Live TTLs: spread over a few far-future deadlines (distinct index buckets).
    groups = min(4, max(1, len(t)))
    for g in range(groups):
        grp = t[g::groups]
        if grp:
            c.cmd("SPEXPIRE", key, FAR_MS + g * 60000, "MEMBERS", len(grp), *grp)
    if h:
        hide_members(c, key, h)
    for mem in p:
        model.m[mem] = PERSIST
    for mem in t:
        model.m[mem] = LIVE_TTL
    for mem in h:
        model.m[mem] = HIDDEN


def mode_model(server, seed, ops):
    rng = random.Random(seed)
    c = server.client(timeout=120)
    disable_active_expire(c)
    c.cmd("FLUSHALL")
    key = "mdl"
    model = SetModel()
    # A mix that forces listpack -> hashtable and single/multi bucket indexes.
    shapes = [(6, 0, 40), (40, 40, 400), (0, 9, 600), (0, 300, 1500),
              (300, 0, 1), (0, 0, 500), (5, 5, 5)]
    checks = 0
    for shape in shapes:
        build_shape(c, model, key, *shape)
        check_views(c, key, model, "build%s" % (shape,))
        check_srandmember(c, key, model, "build%s" % (shape,))
        checks += 1
        for _ in range(ops):
            live = sorted(model.live())
            if not live:
                break
            op = rng.choice(["spop", "spop", "srand", "srem", "hide"])
            if op == "spop":
                do_spop(c, key, model,
                        rng.choice([1, 2, 5, max(1, len(live) // 2), len(live) + 3]),
                        "op-spop")
            elif op == "srand":
                check_srandmember(c, key, model, "op-srand")
            elif op == "srem":
                victims = rng.sample(live, min(len(live), rng.choice([1, 3])))
                c.cmd("SREM", key, *victims)
                for v in victims:
                    del model.m[v]
                check_views(c, key, model, "op-srem")
            elif op == "hide":
                victims = rng.sample(live, min(len(live), rng.choice([1, 5])))
                hide_members(c, key, victims)
                for v in victims:
                    model.m[v] = HIDDEN
                check_views(c, key, model, "op-hide")
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
        for mem in list(model.m):
            if model.m[mem] == HIDDEN:
                del model.m[mem]
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
    return m


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

    def expect_fire(desc, fn):
        nonlocal planted
        try:
            fn()
        except Checker:
            planted += 1
            return
        raise SystemExit("SELFTEST FAILED: checker did not fire for %s" % desc)

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

    print("selftest: %d/7 invariant checkers fired as expected" % planted)
    return planted


# --------------------------------------------------------------------------- #
def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--server", help="path to valkey-server")
    ap.add_argument("--port", type=int, default=27300)
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--ops", type=int, default=40)
    ap.add_argument("--mode", default="all",
                    choices=["model", "persist", "repl", "selftest", "all"])
    ap.add_argument("--debug-assert", action="store_true",
                    help="start the server with --enable-debug-assert yes")
    a = ap.parse_args()

    if a.mode == "selftest":
        sys.exit(0 if mode_selftest() == 7 else 1)

    if not a.server:
        ap.error("--server is required for mode %s" % a.mode)

    # The selftest always runs first (it needs no server and guards the rest).
    if a.mode == "all" and mode_selftest() != 7:
        sys.exit(1)

    base = os.path.join(os.environ.get("KIROCREW_SCRATCH", "/tmp"), "fuzz", "run")
    extra = ["--enable-debug-assert", "yes"] if a.debug_assert else None
    s = Server(a.server, base, "setfuzz-%d" % a.port, a.port, extra=extra).start()
    t0 = time.time()
    try:
        total = 0
        if a.mode in ("model", "all"):
            total += mode_model(s, a.seed, a.ops)
        if a.mode in ("persist", "all"):
            total += mode_persist(s, a.seed)
        if a.mode == "repl":
            print("repl mode lives in repl2.py (needs two servers)", file=sys.stderr)
        print("OK setfuzz mode=%s seed=%d checks=%d %.1fs"
              % (a.mode, a.seed, total, time.time() - t0))
        rc = 0
    except (Checker, ProtocolError, ServerError) as e:
        print("FAIL setfuzz mode=%s seed=%d: %s" % (a.mode, a.seed, e))
        rc = 1
    finally:
        s.close()
    sys.exit(rc)


if __name__ == "__main__":
    main()
