#ifndef MEMORY_PREFETCH_H
#define MEMORY_PREFETCH_H

struct client;
struct serverCommand;
struct serverDb;
struct serverObject;

void prefetchCommandsBatchInit(void);
void processClientsCommandsBatch(void);
int addCommandToBatchAndProcessIfFull(struct client *c);
void removeClientFromPendingCommandsBatch(struct client *c);
int onMaxBatchSizeChange(const char **err);

/* Command-level batches, used by the command ring. */
int prefetchBatchEnabled(void);
/* 'result' is a getKeysResult the caller reuses across the batch. */
int prefetchBatchAddCommand(struct serverCommand *cmd, struct serverObject **argv, int argc, struct serverDb *db,
                            int slot, void *result);
void prefetchBatchRun(void);
void prefetchBatchReset(void);

#endif /* MEMORY_PREFETCH_H */
