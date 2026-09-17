/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#include "workctr.h"

#ifdef WORK_COUNTERS

#include <stdio.h>
#include <string.h>
#include <inttypes.h>

workCounters wc;
workCounters wc_index;
static workCounters wc_last;
static workCounters wc_index_last;
int wc_index_depth = 0;
int wc_alloc_class = WC_CLASS_APP;
int wc_armed = 0;
uint64_t wc_armed_client = 0;
int wc_measuring = 0;

void wcReset(void) {
    memset(&wc, 0, sizeof(wc));
    memset(&wc_index, 0, sizeof(wc_index));
}

static uint64_t wc_generation = 0;

void wcSnapshot(void) {
    wc_last = wc;
    wc_index_last = wc_index;
    wc_generation++;
}

/* Number of snapshots taken so far; ARM replies with the value the next one will have. */
uint64_t wcGeneration(void) {
    return wc_generation;
}

const workCounters *wcLast(void) {
    return &wc_last;
}

const workCounters *wcIndexLast(void) {
    return &wc_index_last;
}

int wcDumpToFile(const char *path) {
    FILE *fp = fopen(path, "w");
    if (!fp) return -1;
#define WC_DUMP(name, desc) fprintf(fp, "%s %" PRId64 "\n", #name, wc.name);
    WC_COUNTERS(WC_DUMP)
#undef WC_DUMP
#define WC_DUMP_IDX(name, desc) fprintf(fp, "idx_%s %" PRId64 "\n", #name, wc_index.name);
    WC_COUNTERS(WC_DUMP_IDX)
#undef WC_DUMP_IDX
    fclose(fp);
    return 0;
}

#else /* !WORK_COUNTERS */

/* Keeps the translation unit non-empty under -pedantic. */
typedef int workctr_disabled_t;

#endif /* WORK_COUNTERS */
