/* lsa.h - log-structured bump allocator for unembedded raw string values.
 * Measurement spike, not mergeable code. Single-threaded: all log mutation
 * happens on the main thread (lazyfree must stay disabled). */
#ifndef LSA_H
#define LSA_H

#include <stdint.h>
#include <stddef.h>
#include "sds.h"

#define LSA_SEGMENT_SIZE (8ULL * 1024 * 1024) /* 8MB segments */
#define LSA_EMAX (1ULL * 1024 * 1024)         /* value admission cap */

/* Lifecycle. lsaInit reserves size_mb of VA (commit on touch) and installs
 * the sds.c hooks. Returns 1 on success, 0 on failure. */
int lsaInit(size_t size_mb);
int lsaEnabled(void);

/* Admission: copy the content of 'src' into a log entry laid out as a
 * regular sds (hdr + data + NUL) so all sds readers work unchanged.
 * Returns the new sds or NULL if not admitted (too big, pool exhausted). */
sds lsaAdmitSds(const_sds src);

/* True if 's' is a log-resident sds. */
int lsaIsLogSds(const_sds s);

/* Register the owner slot: address of the owning robj's val_ptr field.
 * The cleaner rewrites *owner on relocation. */
void lsaSetOwner(sds s, void **owner);

/* Band cleaner slice, called from serverCron. Caller must skip while a
 * child process is active (COW). */
void lsaCleanerCron(void);

/* Append "field:value\r\n" stats lines to an INFO payload. */
sds lsaInfoAppend(sds info);

#endif
