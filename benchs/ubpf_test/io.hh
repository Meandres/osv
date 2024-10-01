#ifndef HELPERS_LOCKING_HH
#define HELPERS_LOCKING_HH
#include <drivers/nvme.hh>

typedef unvme_iod_t iod_t;
typedef uint64_t u64;

extern const unvme_ns_t *ns;

void init();

u64 sync_read(u64 qid, u64 buf, u64 slba, u64 nlb);
iod_t async_read(u64 qid, u64 buf, u64 slba, u64 nlb);

__attribute__((used))
u64 sync_write(u64 qid, u64 buf, u64 slba, u64 nlb);
__attribute__((used))
iod_t async_write(u64 qid, u64 buf, u64 slba, u64 nlb);

__attribute__((used))
u64 poll_completion(iod_t iod);
#endif
