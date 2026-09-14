/*
 * STORE: idempotent retry with a stored reply.
 *
 * STORE <key> PX <ms> | EX <s> primes the connection so that the reply of the
 * *next* command is stored under `key` (as a plain string with a TTL) and
 * replayed verbatim on an identical retry, without re-executing the command.
 *
 * The stored value is: header (magic "VKSR", format version, RESP version,
 * flags, 32-byte SHA-256 fingerprint of db-id + argv, u32 payload length)
 * followed by the raw RESP reply bytes.
 *
 * Local prototype. See valkey-exec-store-onepager.md.
 */

#include "server.h"
#include "sha256.h"

#include <string.h>

/* ---- On-disk / on-wire blob header --------------------------------------- */

#define STORE_MAGIC0 'V'
#define STORE_MAGIC1 'K'
#define STORE_MAGIC2 'S'
#define STORE_MAGIC3 'R'
#define STORE_FORMAT_VERSION 1
#define STORE_FP_LEN 32 /* SHA256_BLOCK_SIZE */

/* Header flags. */
#define STORE_FLAG_OMITTED (1 << 0) /* reply exceeded store-max-reply-bytes; payload absent (tombstone). */

/* Fixed header layout, packed and written/read field by field to avoid any
 * struct-padding / alignment assumptions. Total 43 bytes:
 *   4  magic
 *   1  format version
 *   1  resp version
 *   1  flags
 *   32 fingerprint
 *   4  payload length (u32, little-endian)
 */
#define STORE_HEADER_LEN (4 + 1 + 1 + 1 + STORE_FP_LEN + 4)

/* ---- Fingerprint --------------------------------------------------------- */

/* Canonical length-delimited SHA-256 over db id + every argv of the command.
 * Each field is prefixed with its 8-byte little-endian length so that
 * concatenation is unambiguous. The db id and argc are folded in first. */
static void storeFingerprint(int dbid, robj **argv, int argc, unsigned char out[STORE_FP_LEN]) {
    SHA256_CTX ctx;
    sha256_init(&ctx);

    unsigned char scratch[8];
    uint64_t v;

    v = (uint64_t)dbid;
    for (int i = 0; i < 8; i++) scratch[i] = (v >> (8 * i)) & 0xff;
    sha256_update(&ctx, scratch, 8);

    v = (uint64_t)argc;
    for (int i = 0; i < 8; i++) scratch[i] = (v >> (8 * i)) & 0xff;
    sha256_update(&ctx, scratch, 8);

    for (int j = 0; j < argc; j++) {
        robj *o = getDecodedObject(argv[j]);
        sds s = objectGetVal(o);
        size_t len = sdslen(s);
        v = (uint64_t)len;
        for (int i = 0; i < 8; i++) scratch[i] = (v >> (8 * i)) & 0xff;
        sha256_update(&ctx, scratch, 8);
        sha256_update(&ctx, (const unsigned char *)s, len);
        decrRefCount(o);
    }
    sha256_final(&ctx, out);
}

/* The command as issued by the client, before any internal rewrite (e.g. a
 * served BLPOP is rewritten to LPOP). Fingerprint and replay must both key on
 * this so a served blocking command matches its original retry. */
static void storeClientOrigArgv(client *c, robj ***argv, int *argc) {
    if (c->original_argv) {
        *argv = c->original_argv;
        *argc = c->original_argc;
    } else {
        *argv = c->argv;
        *argc = c->argc;
    }
}

/* ---- Header encode / decode ---------------------------------------------- */

static void storeWriteU32LE(unsigned char *p, uint32_t v) {
    p[0] = v & 0xff;
    p[1] = (v >> 8) & 0xff;
    p[2] = (v >> 16) & 0xff;
    p[3] = (v >> 24) & 0xff;
}

static uint32_t storeReadU32LE(const unsigned char *p) {
    return (uint32_t)p[0] | ((uint32_t)p[1] << 8) | ((uint32_t)p[2] << 16) | ((uint32_t)p[3] << 24);
}

/* Build a blob (header + payload) as an sds. If flags has STORE_FLAG_OMITTED,
 * payload is ignored and a header-only tombstone is written. */
static sds storeBuildBlob(uint8_t resp, uint8_t flags, const unsigned char fp[STORE_FP_LEN], const char *payload, size_t payload_len) {
    unsigned char hdr[STORE_HEADER_LEN];
    size_t off = 0;
    hdr[off++] = STORE_MAGIC0;
    hdr[off++] = STORE_MAGIC1;
    hdr[off++] = STORE_MAGIC2;
    hdr[off++] = STORE_MAGIC3;
    hdr[off++] = STORE_FORMAT_VERSION;
    hdr[off++] = resp;
    hdr[off++] = flags;
    memcpy(hdr + off, fp, STORE_FP_LEN);
    off += STORE_FP_LEN;
    uint32_t plen = (flags & STORE_FLAG_OMITTED) ? 0 : (uint32_t)payload_len;
    storeWriteU32LE(hdr + off, plen);
    off += 4;

    sds blob = sdsnewlen(hdr, STORE_HEADER_LEN);
    if (!(flags & STORE_FLAG_OMITTED) && payload_len) blob = sdscatlen(blob, payload, payload_len);
    return blob;
}

/* Decoded header view. */
typedef struct {
    uint8_t format;
    uint8_t resp;
    uint8_t flags;
    unsigned char fp[STORE_FP_LEN];
    uint32_t payload_len;
} storeHeader;

/* Parse and validate the fixed header of a stored blob. Returns C_OK on a
 * structurally valid header, C_ERR otherwise (bad magic/version/length). */
static int storeParseHeader(sds blob, storeHeader *h) {
    size_t n = sdslen(blob);
    if (n < STORE_HEADER_LEN) return C_ERR;
    const unsigned char *p = (const unsigned char *)blob;
    if (p[0] != STORE_MAGIC0 || p[1] != STORE_MAGIC1 || p[2] != STORE_MAGIC2 || p[3] != STORE_MAGIC3) return C_ERR;
    size_t off = 4;
    h->format = p[off++];
    if (h->format != STORE_FORMAT_VERSION) return C_ERR;
    h->resp = p[off++];
    h->flags = p[off++];
    memcpy(h->fp, p + off, STORE_FP_LEN);
    off += STORE_FP_LEN;
    h->payload_len = storeReadU32LE(p + off);
    off += 4;
    /* Payload length must match the actual trailing bytes, unless tombstone. */
    if (!(h->flags & STORE_FLAG_OMITTED)) {
        if (n != STORE_HEADER_LEN + h->payload_len) return C_ERR;
    } else {
        if (n != STORE_HEADER_LEN) return C_ERR;
    }
    return C_OK;
}

/* Fingerprint for the command about to be stored/replayed on client c.
 * For EXEC this covers db id + every queued command's argv in order + the EXEC
 * argv, so a retry replays only if the whole transaction matches. For any other
 * command it covers db id + the client-issued argv (pre-rewrite). */
static void storeFingerprintForCommand(client *c, unsigned char out[STORE_FP_LEN]) {
    if (c->cmd && c->cmd->proc == execCommand && c->mstate) {
        SHA256_CTX ctx;
        sha256_init(&ctx);
        unsigned char scratch[8];
        uint64_t v;

        v = (uint64_t)c->db->id;
        for (int i = 0; i < 8; i++) scratch[i] = (v >> (8 * i)) & 0xff;
        sha256_update(&ctx, scratch, 8);

        v = (uint64_t)c->mstate->count;
        for (int i = 0; i < 8; i++) scratch[i] = (v >> (8 * i)) & 0xff;
        sha256_update(&ctx, scratch, 8);

        for (int j = 0; j < c->mstate->count; j++) {
            multiCmd *mc = &c->mstate->commands[j];
            v = (uint64_t)mc->argc;
            for (int i = 0; i < 8; i++) scratch[i] = (v >> (8 * i)) & 0xff;
            sha256_update(&ctx, scratch, 8);
            for (int k = 0; k < mc->argc; k++) {
                robj *o = getDecodedObject(mc->argv[k]);
                sds s = objectGetVal(o);
                size_t len = sdslen(s);
                v = (uint64_t)len;
                for (int i = 0; i < 8; i++) scratch[i] = (v >> (8 * i)) & 0xff;
                sha256_update(&ctx, scratch, 8);
                sha256_update(&ctx, (const unsigned char *)s, len);
                decrRefCount(o);
            }
        }
        /* Fold in the EXEC argv itself. */
        v = (uint64_t)c->argc;
        for (int i = 0; i < 8; i++) scratch[i] = (v >> (8 * i)) & 0xff;
        sha256_update(&ctx, scratch, 8);
        for (int k = 0; k < c->argc; k++) {
            robj *o = getDecodedObject(c->argv[k]);
            sds s = objectGetVal(o);
            size_t len = sdslen(s);
            v = (uint64_t)len;
            for (int i = 0; i < 8; i++) scratch[i] = (v >> (8 * i)) & 0xff;
            sha256_update(&ctx, scratch, 8);
            sha256_update(&ctx, (const unsigned char *)s, len);
            decrRefCount(o);
        }
        sha256_final(&ctx, out);
        return;
    }

    robj **fargv;
    int fargc;
    storeClientOrigArgv(c, &fargv, &fargc);
    storeFingerprint(c->db->id, fargv, fargc, out);
}

/* ---- Target lifecycle ---------------------------------------------------- */

void storeClearTarget(client *c) {
    if (c->store_target.key) {
        decrRefCount(c->store_target.key);
        c->store_target.key = NULL;
    }
    c->store_target.ttl_ms = 0;
    c->store_target.armed = 0;
    c->store_target.capture_now = 0;
}

int storeIsTargetArmed(client *c) {
    return c->store_target.armed;
}

/* ---- The STORE command --------------------------------------------------- */

/* STORE key PX ms | EX s */
void storeCommand(client *c) {
    /* A second STORE while one is pending is an error. */
    if (c->store_target.armed) {
        addReplyError(c, "STORE target already pending on this connection");
        return;
    }

    /* Reject in pubsub context; a primed reply capture makes no sense there. */
    if (c->pubsub_data &&
        (hashtableSize(c->pubsub_data->pubsub_channels) || hashtableSize(c->pubsub_data->pubsub_patterns) ||
         hashtableSize(c->pubsub_data->pubsubshard_channels))) {
        addReplyError(c, "STORE is not allowed in subscribe context");
        return;
    }

    long long ttl;
    if (getLongLongFromObjectOrReply(c, c->argv[3], &ttl, NULL) != C_OK) return;

    char *unit = objectGetVal(c->argv[2]);
    long long ttl_ms;
    if (!strcasecmp(unit, "PX")) {
        ttl_ms = ttl;
    } else if (!strcasecmp(unit, "EX")) {
        if (ttl > LLONG_MAX / 1000) {
            addReplyError(c, "invalid expire time in 'store' command");
            return;
        }
        ttl_ms = ttl * 1000;
    } else {
        addReplyErrorObject(c, shared.syntaxerr);
        return;
    }

    if (ttl_ms <= 0) {
        addReplyError(c, "invalid expire time in 'store' command");
        return;
    }

    /* Arm the target. */
    c->store_target.key = c->argv[1];
    incrRefCount(c->store_target.key);
    c->store_target.ttl_ms = ttl_ms;
    c->store_target.armed = 1;

    /* +OK if no stored reply exists yet, +REPLAY if one does. The blob is not
     * validated here; that happens when the next command arrives. */
    robj *existing = lookupKeyReadWithFlags(c->db, c->store_target.key, LOOKUP_NONOTIFY | LOOKUP_NOTOUCH);
    if (existing == NULL) {
        addReplyStatus(c, "OK");
    } else {
        addReplyStatus(c, "REPLAY");
    }
}

/* ---- Not-storable command classification --------------------------------- */

/* Commands whose reply must never be captured/replayed. Returns 1 if not
 * storable. */
int storeCommandIsNotStorable(client *c) {
    struct serverCommand *cmd = c->cmd;
    if (cmd == NULL) return 0;
    if (cmd->proc == storeCommand) return 1;
    if (cmd->proc == subscribeCommand || cmd->proc == psubscribeCommand || cmd->proc == ssubscribeCommand) return 1;
    if (cmd->proc == monitorCommand) return 1;
    if (cmd->proc == syncCommand) return 1; /* SYNC and PSYNC share syncCommand. */
    /* CLIENT REPLY OFF|SKIP mutate the reply stream. */
    if (cmd->proc == clientCommand && c->argc >= 2 && !strcasecmp(objectGetVal(c->argv[1]), "reply")) return 1;
    return 0;
}

/* ---- Reply capture -------------------------------------------------------
 *
 * The reply is captured non-destructively: the command writes its reply to the
 * client's real buffers as usual (so it is sent normally), and we record the
 * reply boundary before the command runs, then extract the delta afterwards.
 * Copy-avoidance is disabled for the window so the captured bytes are plain,
 * contiguous RESP with no payloadHeader framing or bulk-string references. */
typedef struct {
    size_t bufpos;          /* c->bufpos before the command. */
    listNode *reply_tail;   /* last reply-list node before the command (or NULL). */
    size_t reply_tail_used; /* that node's used bytes before the command. */
    int saved_min_string_copy;
    int saved_min_string_copy_threaded;
    int saved_min_io_threads_copy;
    int saved_buf_encoded;
    unsigned char fp[STORE_FP_LEN]; /* fingerprint computed at begin (pre-exec). */
    int active;
} storeReplySnapshot;

/* Per-client capture state. A connection processes commands serially and STORE
 * captures at most one reply at a time (never nested), so a single static is
 * safe. */
static storeReplySnapshot store_snapshot;

/* Record the reply boundary and disable copy-avoidance so the primed command's
 * reply can later be extracted verbatim as a delta. */
void storeCaptureBegin(client *c) {
    serverAssert(!store_snapshot.active);
    /* Compute the fingerprint now, before the command executes and (for EXEC)
     * frees its queued command state. */
    storeFingerprintForCommand(c, store_snapshot.fp);

    store_snapshot.bufpos = c->bufpos;
    listNode *tail = listLast(c->reply);
    store_snapshot.reply_tail = tail;
    store_snapshot.reply_tail_used = tail ? ((clientReplyBlock *)listNodeValue(tail))->used : 0;

    store_snapshot.saved_min_string_copy = server.min_string_size_copy_avoid;
    store_snapshot.saved_min_string_copy_threaded = server.min_string_size_copy_avoid_threaded;
    store_snapshot.saved_min_io_threads_copy = server.min_io_threads_copy_avoid;
    store_snapshot.saved_buf_encoded = c->flag.buf_encoded;
    server.min_string_size_copy_avoid = 0;
    server.min_string_size_copy_avoid_threaded = 0;
    server.min_io_threads_copy_avoid = 0;

    store_snapshot.active = 1;
}

/* Extract the reply bytes the command appended since storeCaptureBegin() as a
 * contiguous sds. Copy-avoidance was disabled, so every relevant block is plain. */
static sds storeExtractCapturedReply(client *c) {
    sds out = sdsempty();

    /* Bytes appended to the static buffer beyond the snapshot position. */
    if (c->bufpos > store_snapshot.bufpos) {
        serverAssert(!c->flag.buf_encoded);
        out = sdscatlen(out, c->buf + store_snapshot.bufpos, c->bufpos - store_snapshot.bufpos);
    }

    /* Reply-list contribution: the grown portion of the pre-existing tail node,
     * then every node appended after it. */
    int past_tail = (store_snapshot.reply_tail == NULL);
    listIter it;
    listNode *ln;
    listRewind(c->reply, &it);
    while ((ln = listNext(&it))) {
        clientReplyBlock *o = listNodeValue(ln);
        if (!past_tail) {
            if (ln == store_snapshot.reply_tail) {
                past_tail = 1;
                if (o->used > store_snapshot.reply_tail_used) {
                    serverAssert(!o->flag.buf_encoded);
                    out = sdscatlen(out, o->buf + store_snapshot.reply_tail_used,
                                    o->used - store_snapshot.reply_tail_used);
                }
            }
            continue;
        }
        if (o->used) {
            serverAssert(!o->flag.buf_encoded);
            out = sdscatlen(out, o->buf, o->used);
        }
    }
    return out;
}

/* Restore the copy-avoidance config after a capture. The client's own buffers
 * are untouched (non-destructive capture). */
static void storeCaptureRestore(client *c) {
    UNUSED(c);
    if (!store_snapshot.active) return;
    server.min_string_size_copy_avoid = store_snapshot.saved_min_string_copy;
    server.min_string_size_copy_avoid_threaded = store_snapshot.saved_min_string_copy_threaded;
    server.min_io_threads_copy_avoid = store_snapshot.saved_min_io_threads_copy;
    store_snapshot.active = 0;
}

/* Abort a capture: nothing was appended to the client (the primed command
 * blocked without replying), just restore config. */
void storeCaptureRollback(client *c) {
    storeCaptureRestore(c);
}

/* Persist a blob under the target key with the target TTL and propagate a
 * SET key blob PXAT <deadline> so replicas/AOF get the marker in the same
 * execution unit as the command's own effects. */
static void storePersistBlob(client *c, sds blob) {
    robj *key = c->store_target.key;
    long long deadline = commandTimeSnapshot() + c->store_target.ttl_ms;

    robj *val = createObject(OBJ_STRING, sdsdup(blob));
    /* setKey takes ownership of val's reference (it is not incremented), so we
     * must not decrRefCount it here. It may also reallocate *valref. */
    setKey(c, c->db, key, &val, SETKEY_NO_SIGNAL);
    setExpire(c, c->db, key, deadline);
    signalModifiedKey(c, c->db, key);
    notifyKeyspaceEvent(NOTIFY_STRING, "set", key, c->db->id);
    notifyKeyspaceEvent(NOTIFY_GENERIC, "expire", key, c->db->id);

    /* Propagate an explicit SET ... PXAT so it rides the command's exec unit. */
    robj *argv[5];
    argv[0] = shared.set;
    argv[1] = key;
    argv[2] = createObject(OBJ_STRING, sdsdup(blob));
    argv[3] = shared.pxat;
    argv[4] = createStringObjectFromLongLong(deadline);
    alsoPropagate(c->db->id, argv, 5, PROPAGATE_AOF | PROPAGATE_REPL, c->slot);
    decrRefCount(argv[2]);
    decrRefCount(argv[4]);

    server.dirty++;
}

/* Replay a stored reply: write bytes verbatim to the client. */
static void storeReplayReply(client *c, sds blob, const storeHeader *h) {
    addReplyProto(c, blob + STORE_HEADER_LEN, h->payload_len);
}

/* Called from processCommand just before a primed command would execute.
 * Returns 1 if the command must NOT execute (a reply/error was already sent and
 * the target consumed): replay hit, mismatch, invalid blob, proto mismatch,
 * omitted tombstone, or a not-storable command. Returns 0 if the command should
 * execute normally and its reply be captured. */
int storeHandleReplayBeforeCall(client *c) {
    if (!c->store_target.armed) return 0;

    /* A second STORE while one is pending must reach storeCommand so it can
     * report the pending target and leave it intact. Do not consume here. */
    if (c->cmd && c->cmd->proc == storeCommand) return 0;

    /* MULTI carries the target through to EXEC untouched: its +OK and the
     * queued commands' +QUEUED replies are not stored. Do not consume. */
    if (c->cmd && c->cmd->proc == multiCommand) return 0;

    /* Not-storable commands: error on the slot, execute nothing, drop target. */
    if (storeCommandIsNotStorable(c)) {
        addReplyError(c, "command reply cannot be stored for idempotent retry");
        storeClearTarget(c);
        return 1;
    }

    robj *key = c->store_target.key;
    robj *existing = lookupKeyReadWithFlags(c->db, key, LOOKUP_NONOTIFY | LOOKUP_NOTOUCH);
    if (existing == NULL) return 0; /* absent -> execute and capture. */

    /* From here we will not execute the command. If it is EXEC, tear down the
     * transaction state first so the client is left in a clean, non-multi
     * state (the queued commands are not run). */
    int is_exec = c->cmd && c->cmd->proc == execCommand;
#define STORE_SKIP_EXEC()                   \
    do {                                    \
        if (is_exec) discardTransaction(c); \
        storeClearTarget(c);                \
        return 1;                           \
    } while (0)

    if (existing->type != OBJ_STRING || !sdsEncodedObject(existing)) {
        addReplyError(c, "STOREINVALID stored reply is not a valid blob");
        STORE_SKIP_EXEC();
    }
    sds blob = objectGetVal(existing);

    storeHeader h;
    if (storeParseHeader(blob, &h) != C_OK) {
        addReplyError(c, "STOREINVALID corrupt stored reply");
        STORE_SKIP_EXEC();
    }

    /* Fingerprint must match this exact command (db id + all argv; for EXEC,
     * every queued command plus the EXEC argv). */
    unsigned char fp[STORE_FP_LEN];
    storeFingerprintForCommand(c, fp);
    if (memcmp(fp, h.fp, STORE_FP_LEN) != 0) {
        addReplyError(c, "STOREMISMATCH stored reply belongs to a different command");
        STORE_SKIP_EXEC();
    }

    /* RESP version must match. */
    if (h.resp != c->resp) {
        addReplyError(c, "STOREPROTO stored reply used a different RESP version");
        STORE_SKIP_EXEC();
    }

    /* Oversized tombstone: the command already ran once, its reply is gone. */
    if (h.flags & STORE_FLAG_OMITTED) {
        addReplyError(c, "STOREOMITTED stored reply exceeded store-max-reply-bytes");
        STORE_SKIP_EXEC();
    }

    /* Replay verbatim, execute nothing, propagate nothing. */
    storeReplayReply(c, blob, &h);
    STORE_SKIP_EXEC();
#undef STORE_SKIP_EXEC
}

/* Called from call() right after c->cmd->proc(c) to capture and persist. */
void storeCaptureCommit(client *c) {
    /* Extract the reply the command just appended (already sent to the client;
     * we only take a copy to store). */
    sds reply = storeExtractCapturedReply(c);

    /* Fingerprint was computed at capture-begin, before the command executed
     * (and before EXEC freed its queued command state). */
    unsigned char fp[STORE_FP_LEN];
    memcpy(fp, store_snapshot.fp, STORE_FP_LEN);

    storeCaptureRestore(c);

    uint8_t flags = 0;
    if ((long long)sdslen(reply) > server.store_max_reply_bytes) flags |= STORE_FLAG_OMITTED;

    sds blob = storeBuildBlob(c->resp, flags, fp, reply, sdslen(reply));
    storePersistBlob(c, blob);
    sdsfree(blob);
    sdsfree(reply);

    storeClearTarget(c);
}
