static unsigned long (*sync_write) (unsigned long qid, unsigned long buf, unsigned long slba, unsigned long nlb) = (void*)1;
static unsigned long (*async_write) (unsigned long qid, unsigned long buf, unsigned long slba, unsigned long nlb) = (void*)2;
static unsigned long (*poll) (unsigned long iod_t) = (void*)3;

int write_1(void *mem, unsigned size) {
    sync_write(0, (unsigned long)mem, 0, size/512);
    return 0;
}

int write_2(void *mem, unsigned size) {
    unsigned long iod = async_write(0, (unsigned long)mem, 0, size/512);
    return poll(iod);
}
