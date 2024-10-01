#include "io.hh"

const unvme_ns_t *ns;

void init(){
    ns = unvme_open();
}

u64 sync_read(u64 qid, u64 buf, u64 slba, u64 nlb){
    if(ns==NULL){
        init();
    }
    return unvme_read(ns, qid, reinterpret_cast<void*>(buf), slba, nlb);
}

iod_t async_read(u64 qid, u64 buf, u64 slba, u64 nlb){
    if(ns==NULL){
        init();
    }
    return unvme_aread(ns, qid, reinterpret_cast<void*>(buf), slba, nlb);
}

u64 sync_write(u64 qid, u64 buf, u64 slba, u64 nlb){
    if(ns==NULL){
        init();
    }
    return unvme_write(ns, qid, reinterpret_cast<const void*>(buf), slba, nlb);
}

iod_t async_write(u64 qid, u64 buf, u64 slba, u64 nlb){
    if(ns==NULL){
        init();
    }
    return unvme_awrite(ns, qid, reinterpret_cast<const void*>(buf), slba, nlb);
}

u64 poll_completion(iod_t iod){
    return unvme_apoll(iod, UNVME_SHORT_TIMEOUT);
}
