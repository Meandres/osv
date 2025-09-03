/**
 * Copyright (c) 2015-2016, Micron Technology, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   1. Redistributions of source code must retain the above copyright
 *      notice, this list of conditions and the following disclaimer.
 *
 *   2. Redistributions in binary form must reproduce the above copyright
 *      notice, this list of conditions and the following disclaimer in the
 *      documentation and/or other materials provided with the distribution.
 *
 *   3. Neither the name of the copyright holder nor the names of its
 *      contributors may be used to endorse or promote products derived
 *      from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#include <stdarg.h>
#include <pthread.h>
#include <sys/mman.h>
#include <string.h>
#include <signal.h>
#include <errno.h>
#include <sched.h>

#include "drivers/poll_nvme.hh"
#include <sys/cdefs.h>
#include "drivers/pci-device.hh"
#include <osv/interrupt.hh>
#include <osv/virt_to_phys.hh>

#include <cassert>
#include <sstream>
#include <string>
#include <map>
#include <osv/debug.h>

#include <osv/sched.hh>
#include <osv/trace.hh>
#include <osv/aligned_new.hh>

#include "drivers/clock.hh"
#include "drivers/clockevent.hh"

#include <osv/device.h>
#include <osv/ioctl.h>
#include <osv/contiguous_alloc.hh>
#include <osv/mmu.hh>
#include <sys/time.h>
#include <osv/ucache.hh>

#include <atomic>

// Static global variables
static FILE*                log_fp = NULL;  ///< log file pointer
static int                  log_count = 0;  ///< log open count
static pthread_mutex_t      log_lock = PTHREAD_MUTEX_INITIALIZER; ///< log lock
nvme_controller_reg_t* globalReg;
int nvme::__disk_idx = 0;
//std::vector<u32> last_cq_doorbelled;

/// IO descriptor debug print
#define PDEBUG(fmt, arg...) //fprintf(stderr, fmt "\n", ##arg)

/**
 * Open log file.  Only one log file is supported and thus only the first
 * call * will create the log file by its specified name.  Subsequent calls
 * will only be counted.
 * @param   name        log filename
 * @param   mode        open mode
 * @return  0 indicating 
 */
int log_open(const char* name, const char* mode)
{
   return 0;
    pthread_mutex_lock(&log_lock);
    if (!log_fp) {
        log_fp = fopen(name, mode);
        if (!log_fp) {
            perror("log_open");
            pthread_mutex_unlock(&log_lock);
            return -1;
        }
    }
    log_count++;
    pthread_mutex_unlock(&log_lock);
    return 0;
}

/**
 * Close the log file (only the last close will effectively close the file).
 */
void log_close()
{
    pthread_mutex_lock(&log_lock);
    if (log_count > 0) {
        if ((--log_count == 0) && log_fp && log_fp != stdout) {
            fclose(log_fp);
            log_fp = NULL;
        }
    }
    pthread_mutex_unlock(&log_lock);
}

/**
 * Write a formatted message to log file, if log file is opened.
 * If err flag is set then log also to stderr.
 * @param   ftee        additional file to print to
 * @param   fmt         formatted message
 */
void log_msg(FILE* ftee, const char* fmt, ...)
{
    va_list args;

    pthread_mutex_lock(&log_lock);
    if (log_fp) {
        va_start(args, fmt);
        if (ftee) {
            char s[4096];
            vsnprintf(s, sizeof(s), fmt, args);
            fprintf(ftee, "%s", s);
            fflush(ftee);
            fprintf(log_fp, "%s", s);
            fflush(log_fp);
        } else {
            vfprintf(log_fp, fmt, args);
            fflush(log_fp);
        }
        va_end(args);
    } else {
        va_start(args, fmt);
        if (ftee) {
            vfprintf(ftee, fmt, args);
            fflush(ftee);
        } else {
            vfprintf(stdout, fmt, args);
            fflush(stdout);
        }
        va_end(args);
    }
    pthread_mutex_unlock(&log_lock);
}


static inline u32 r32(nvme_device_t* dev, u32* addr)
{
    u32 val = *addr;
    DEBUG("r32 %#lx %#x", (u64) addr - (u64) dev->reg, val);
    //ucache::mmioAccesses++;
    return val;
}

static inline void w32(nvme_device_t* dev, u32* addr, u32 val)
{
    DEBUG("w32 %#lx %#x", (u64) addr - (u64) dev->reg, val);
    *addr = val;
    //ucache::mmioAccesses++;
}

static inline u64 r64(nvme_device_t* dev, u64* addr)
{
    u64 val = *addr;
    DEBUG("r64 %#lx %#lx", (u64) addr - (u64) dev->reg, val);
    //ucache::mmioAccesses++;
    return val;
}

static inline void w64(nvme_device_t* dev, u64* addr, u64 val)
{
    DEBUG("w64 %#lx %#lx", (u64) addr - (u64) dev->reg, val);
    *addr = val;
    //ucache::mmioAccesses++;
}

/**
 * Wait for controller enabled/disabled state.
 * @param   dev         device context
 * @param   ready       ready state (enabled/disabled)
 * @return  0 if ok else -1.
 */
static int nvme_ctlr_wait_ready(nvme_device_t* dev, int ready)
{
    int i;
    for (i = 0; i < dev->timeout; i++) {
        usleep(500000);
        nvme_controller_status_t csts;
        csts.val = r32(dev, &dev->reg->csts.val);
        if (csts.rdy == ready) return 0;
    }

    ERROR("timeout waiting for ready %d", ready);
    return -1;
}

/**
 * Disable controller.
 * @param   dev         device context
 * @return  0 if ok else -1.
 */
static int nvme_ctlr_disable(nvme_device_t* dev)
{
    DEBUG_FN();
    nvme_controller_config_t cc;
    cc.val = r32(dev, &dev->reg->cc.val);
    cc.en = 0;
    w32(dev, &dev->reg->cc.val, cc.val);
    return nvme_ctlr_wait_ready(dev, 0);
}

/**
 * Enable controller.
 * @param   dev         device context
 * @param   cc          controller configuration settings
 * @return  0 if ok else -1.
 */
static int nvme_ctlr_enable(nvme_device_t* dev, nvme_controller_config_t cc)
{
    DEBUG_FN();
    cc.en = 1;
    w32(dev, &dev->reg->cc.val, cc.val);
    return nvme_ctlr_wait_ready(dev, 1);
}

/**
 * Submit an entry at submission queue tail.
 * @param   q           queue
 * @return  0 if ok else -1.
 */
static int nvme_submit_cmd(nvme_queue_t* q, bool ring=true)
{
    int tail = q->sq_tail;
    //HEX_DUMP(&q->sq[tail], sizeof(nvme_sq_entry_t));
    if (++tail == q->size) tail = 0;
#if 0
    // Some SSD does not advance sq_head properly (e.g. Intel DC D3600)
    // so let the upper layer detect queue full error condition
    if (tail == q->sq_head) {
        ERROR("sq full at %d", tail);
        return -1;
    }
#endif
    q->sq_tail = tail;
    if(ring)
        w32(q->dev, q->sq_doorbell, tail);
    return 0;
}

void unvme_ring_sq_doorbell(const unvme_ns_t* ns, int qid){
    unvme_queue_t* q = ((unvme_session_t*)ns->ses)->dev->ioqs + qid;
    nvme_queue_t* nvmeq = q->nvmeq;
    w32(nvmeq->dev, nvmeq->sq_doorbell, nvmeq->sq_tail);
}

/**
 * Check a completion queue and return the completed command id and status.
 * @param   q           queue
 * @param   stat        completion status returned
 * @param   cqe_cs      CQE command specific DW0 returned
 * @return  the completed command id or -1 if there's no completion.
 */
int nvme_check_completion(nvme_queue_t* q, int* stat, u32* cqe_cs)
{
    *stat = 0;
    nvme_cq_entry_t* cqe = &q->cq[q->cq_head];
    if (cqe->p == q->cq_phase) return -1;

    *stat = cqe->psf & 0xfffe;
    if (++q->cq_head == q->size) {
        q->cq_head = 0;
        q->cq_phase = !q->cq_phase;
    }
    if (cqe_cs) *cqe_cs = cqe->cs;
   
    if(!ucache::batch_io_request){
        w32(q->dev, q->cq_doorbell, q->cq_head);
    }else{
        if(q->id == 0){ // adminq
            w32(q->dev, q->cq_doorbell, q->cq_head);
        }else{
            if((unsigned)q->cq_head == q->last_cq_doorbelled){
                q->last_cq_doorbelled = q->cq_head == 0 ? q->size-1 : q->cq_head-1;
                w32(q->dev, q->cq_doorbell, q->cq_head);
            }
        }
    }

#if 0
    // Some SSD does not advance sq_head properly (e.g. Intel DC D3600)
    // so let the upper layer detect queue full error condition
    q->sq_head = cqe->sqhd;
#endif

    if (*stat == 0) {
        PDEBUG("q=%d cq=%d sq=%d-%d cid=%#x (C)", q->id, q->cq_head, q->sq_head, q->sq_tail, cqe->cid);
    } else {
        printf("current cpu id: %u\n", sched::cpu::current()->id);
        ERROR("q=%d cq=%d sq=%d-%d cid=%#x stat=%#x (dnr=%d m=%d sct=%d sc=%#x) (C)",
              q->id, q->cq_head, q->sq_head, q->sq_tail, cqe->cid, *stat, cqe->dnr, cqe->m, cqe->sct, cqe->sc);
    }
    return cqe->cid;
}

/**
 * Wait for a given command completion until timeout.
 * @param   q           queue
 * @param   cid         cid
 * @param   timeout     timeout in seconds
 * @return  completion status (0 if ok).
 */
int nvme_wait_completion(nvme_queue_t* q, int cid, int timeout)
{
    u64 endtsc = 0;

    do {
        int stat;
        int ret = nvme_check_completion(q, &stat, NULL);
        if (ret >= 0) {
            if (ret == cid && stat == 0) return 0;
            if (ret != cid) {
                ERROR("cid wait=%#x recv=%#x", cid, ret);
                stat = -1;
            }
            return stat;
        } else if (endtsc == 0) {
            endtsc = processor::rdtsc() + timeout * q->dev->rdtsec;
        }
    } while (processor::rdtsc() < endtsc);

    ERROR("timeout");
    return -1;
}

/**
 * NVMe identify command.
 * Submit the command and wait for completion.
 * @param   dev         device context
 * @param   nsid        namespace id (< 0 implies cns)
 * @param   prp1        PRP1 address
 * @param   prp2        PRP2 address
 * @return  completion status (0 if ok).
 */
int nvme_acmd_identify(nvme_device_t* dev, int nsid, u64 prp1, u64 prp2)
{
    nvme_queue_t* adminq = &dev->adminq;
    int cid = adminq->sq_tail;
    nvme_acmd_identify_t* cmd = &adminq->sq[cid].identify;

    memset(cmd, 0, sizeof (*cmd));
    cmd->common.opc = NVME_ACMD_IDENTIFY;
    cmd->common.cid = cid;
    cmd->common.nsid = nsid;
    cmd->common.prp1 = prp1;
    cmd->common.prp2 = prp2;
    cmd->cns = nsid == 0 ? 1 : 0;

    DEBUG_FN("sq=%d-%d cid=%#x nsid=%d", adminq->sq_head, adminq->sq_tail, cid, nsid);
    int err = nvme_submit_cmd(adminq);
    if (!err) err = nvme_wait_completion(adminq, cid, 30);
    return err;
}

/**
 * NVMe get log page command.
 * Submit the command and wait for completion.
 * @param   dev         device context
 * @param   nsid        namespace id
 * @param   lid         log page id
 * @param   numd        number of dwords
 * @param   prp1        PRP1 address
 * @param   prp2        PRP2 address
 * @return  completion status (0 if ok).
 */
int nvme_acmd_get_log_page(nvme_device_t* dev, int nsid,
                           int lid, int numd, u64 prp1, u64 prp2)
{
    nvme_queue_t* adminq = &dev->adminq;
    int cid = adminq->sq_tail;
    nvme_acmd_get_log_page_t* cmd = &adminq->sq[cid].get_log_page;

    memset(cmd, 0, sizeof (*cmd));
    cmd->common.opc = NVME_ACMD_GET_LOG_PAGE;
    cmd->common.cid = cid;
    cmd->common.nsid = nsid;
    cmd->common.prp1 = prp1;
    cmd->common.prp2 = prp2;
    cmd->lid = lid;
    cmd->numd = numd;

    DEBUG_FN("sq=%d-%d cid=%#x lid=%d", adminq->sq_head, adminq->sq_tail, cid, lid);
    int err = nvme_submit_cmd(adminq);
    if (!err) err = nvme_wait_completion(adminq, cid, 30);
    return err;
}

/**
 * NVMe get features command.
 * Submit the command and wait for completion.
 * @param   dev         device context
 * @param   nsid        namespace id
 * @param   fid         feature id
 * @param   prp1        PRP1 address
 * @param   prp2        PRP2 address
 * @param   res         dword 0 value returned
 * @return  completion status (0 if ok).
 */
int nvme_acmd_get_features(nvme_device_t* dev, int nsid,
                           int fid, u64 prp1, u64 prp2, u32* res)
{
    nvme_queue_t* adminq = &dev->adminq;
    int cid = adminq->sq_tail;
    nvme_acmd_get_features_t* cmd = &adminq->sq[cid].get_features;

    memset(cmd, 0, sizeof (*cmd));
    cmd->common.opc = NVME_ACMD_GET_FEATURES;
    cmd->common.cid = cid;
    cmd->common.nsid = nsid;
    cmd->common.prp1 = prp1;
    cmd->common.prp2 = prp2;
    cmd->fid = fid;
    *res = -1;

    DEBUG_FN("sq=%d-%d cid=%#x fid=%d", adminq->sq_head, adminq->sq_tail, cid, fid);
    int err = nvme_submit_cmd(adminq);
    if (!err) err = nvme_wait_completion(adminq, cid, 30);
    if (!err) *res = adminq->cq[cid].cs;
    return err;
}

/**
 * NVMe set features command.
 * Submit the command and wait for completion.
 * @param   dev         device context
 * @param   nsid        namespace id
 * @param   fid         feature id
 * @param   prp1        PRP1 address
 * @param   prp2        PRP2 address
 * @param   res         dword 0 value returned
 * @return  completion status (0 if ok).
 */
int nvme_acmd_set_features(nvme_device_t* dev, int nsid,
                           int fid, u64 prp1, u64 prp2, u32* res)
{
    nvme_queue_t* adminq = &dev->adminq;
    int cid = adminq->sq_tail;
    nvme_acmd_set_features_t* cmd = &adminq->sq[cid].set_features;

    memset(cmd, 0, sizeof (*cmd));
    cmd->common.opc = NVME_ACMD_SET_FEATURES;
    cmd->common.cid = cid;
    cmd->common.nsid = nsid;
    cmd->common.prp1 = prp1;
    cmd->common.prp2 = prp2;
    cmd->fid = fid;
    cmd->val = *res;
    *res = -1;

    DEBUG_FN("t=%d h=%d cid=%#x fid=%d", adminq->sq_tail, adminq->sq_head, cid, fid);
    int err = nvme_submit_cmd(adminq);
    if (!err) err = nvme_wait_completion(adminq, cid, 30);
    if (!err) *res = adminq->cq[cid].cs;
    return err;
}

/**
 * NVMe create I/O completion queue command.
 * Submit the command and wait for completion.
 * @param   ioq         io queue
 * @param   prp         PRP1 address
 * @return  0 if ok, else -1.
 */
int nvme_acmd_create_cq(nvme_queue_t* ioq, u64 prp)
{
    nvme_queue_t* adminq = &ioq->dev->adminq;
    int cid = adminq->sq_tail;
    nvme_acmd_create_cq_t* cmd = &adminq->sq[cid].create_cq;

    memset(cmd, 0, sizeof (*cmd));
    cmd->common.opc = NVME_ACMD_CREATE_CQ;
    cmd->common.cid = cid;
    cmd->common.prp1 = prp;
    cmd->pc = 1;
    cmd->qid = ioq->id;
    cmd->qsize = ioq->size - 1;

    DEBUG_FN("sq=%d-%d cid=%#x cq=%d qs=%d", adminq->sq_head, adminq->sq_tail, cid, ioq->id, ioq->size);
    int err = nvme_submit_cmd(adminq);
    if (!err) err = nvme_wait_completion(adminq, cid, 30);
    return err;
}

/**
 * NVMe create I/O submission queue command.
 * Submit the command and wait for completion.
 * @param   ioq         io queue
 * @param   prp         PRP1 address
 * @return  0 if ok, else -1.
 */
int nvme_acmd_create_sq(nvme_queue_t* ioq, u64 prp)
{
    nvme_queue_t* adminq = &ioq->dev->adminq;
    int cid = adminq->sq_tail;
    nvme_acmd_create_sq_t* cmd = &adminq->sq[cid].create_sq;

    memset(cmd, 0, sizeof (*cmd));
    cmd->common.opc = NVME_ACMD_CREATE_SQ;
    cmd->common.cid = cid;
    cmd->common.prp1 = prp;
    cmd->pc = 1;
    cmd->qprio = 2; // 0=urgent 1=high 2=medium 3=low
    cmd->qid = ioq->id;
    cmd->cqid = ioq->id;
    cmd->qsize = ioq->size - 1;

    DEBUG_FN("sq=%d-%d cid=%#x cq=%d qs=%d", adminq->sq_head, adminq->sq_tail, cid, ioq->id, ioq->size);
    int err = nvme_submit_cmd(adminq);
    if (!err) err = nvme_wait_completion(adminq, cid, 30);
    return err;
}

/**
 * NVMe delete I/O queue command.
 * Submit the command and wait for completion.
 * @param   ioq         io queue
 * @param   opc         op code
 * @return  0 if ok else error code.
 */
static inline int nvme_acmd_delete_ioq(nvme_queue_t* ioq, int opc)
{
    nvme_queue_t* adminq = &ioq->dev->adminq;
    int cid = adminq->sq_tail;
    nvme_acmd_delete_ioq_t* cmd = &adminq->sq[cid].delete_ioq;

    memset(cmd, 0, sizeof (*cmd));
    cmd->common.opc = opc;
    cmd->common.cid = cid;
    cmd->qid = ioq->id;

    DEBUG_FN("sq=%d-%d cid=%#x %cq=%d", adminq->sq_head, adminq->sq_tail, cid,
             opc == NVME_ACMD_DELETE_CQ ? 'c' : 's', ioq->id);
    int err = nvme_submit_cmd(adminq);
    if (!err) err = nvme_wait_completion(adminq, cid, 30);
    return err;
}

/**
 * NVMe delete I/O completion queue command.
 * Submit the command and wait for completion.
 * @param   ioq         io queue
 * @return  0 if ok else error code.
 */
int nvme_acmd_delete_cq(nvme_queue_t* ioq)
{
    return nvme_acmd_delete_ioq(ioq, NVME_ACMD_DELETE_CQ);
}

/**
 * NVMe delete I/O submission queue command.
 * Submit the command and wait for completion.
 * @param   ioq         io queue
 * @return  0 if ok else error code.
 */
int nvme_acmd_delete_sq(nvme_queue_t* ioq)
{
    return nvme_acmd_delete_ioq(ioq, NVME_ACMD_DELETE_SQ);
}

/**
 * NVMe submit a vendor specific (i.e. generic) command.
 * @param   q           NVMe (admin or IO) queue
 * @param   opc         vendor specific op code
 * @param   cid         command id
 * @param   nsid        namespace
 * @param   prp1        PRP1 address
 * @param   prp2        PRP2 address
 * @param   cdw10_15    command dwords 10-15
 * @return  0 if ok else -1.
 */
int nvme_cmd_vs(nvme_queue_t* q, int opc, u16 cid, int nsid,
                u64 prp1, u64 prp2, u32 cdw10_15[6])
{
    nvme_command_vs_t* cmd = &q->sq[q->sq_tail].vs;

    memset(cmd, 0, sizeof (*cmd));
    cmd->common.opc = opc;
    cmd->common.cid = cid;
    cmd->common.nsid = nsid;
    cmd->common.prp1 = prp1;
    cmd->common.prp2 = prp2;
    if (cdw10_15) memcpy(cmd->cdw10_15, cdw10_15, sizeof(cmd->cdw10_15));
    DEBUG_FN("q=%d sq=%d-%d cid=%#x nsid=%d opc=%#x",
             q->id, q->sq_head, q->sq_tail, cid, nsid, opc);
    return nvme_submit_cmd(q);
}

/**
 * NVMe submit a read write command.
 * @param   ioq         io queue
 * @param   opc         op code
 * @param   cid         command id
 * @param   nsid        namespace
 * @param   slba        startling logical block address
 * @param   nlb         number of logical blocks
 * @param   prp1        PRP1 address
 * @param   prp2        PRP2 address
 * @return  0 if ok else -1.
 */
int nvme_cmd_rw(nvme_queue_t* ioq, int opc, u16 cid, int nsid,
                u64 slba, int nlb, u64 prp1, u64 prp2, bool ring)
{
    nvme_command_rw_t* cmd = &ioq->sq[ioq->sq_tail].rw;

    memset(cmd, 0, sizeof (*cmd));
    cmd->common.opc = opc;
    cmd->common.cid = cid;
    cmd->common.nsid = nsid;
    cmd->common.prp1 = prp1;
    cmd->common.prp2 = prp2;
    cmd->slba = slba;
    cmd->nlb = nlb - 1;
    DEBUG_FN("q=%d sq=%d-%d cid=%#x nsid=%d lba=%#lx nb=%#x prp=%#lx.%#lx (%c)",
             ioq->id, ioq->sq_head, ioq->sq_tail, cid, nsid, slba, nlb, prp1, prp2,
             opc == NVME_CMD_READ? 'R' : 'W');
    return nvme_submit_cmd(ioq, ring);
}

/**
 * NVMe submit a read command.
 * @param   ioq         io queue
 * @param   cid         command id
 * @param   nsid        namespace
 * @param   slba        startling logical block address
 * @param   nlb         number of logical blocks
 * @param   prp1        PRP1 address
 * @param   prp2        PRP2 address
 * @return  0 if ok else -1.
 */
int nvme_cmd_read(nvme_queue_t* ioq, u16 cid, int nsid,
                  u64 slba, int nlb, u64 prp1, u64 prp2, bool ring=true)
{
    return nvme_cmd_rw(ioq, NVME_CMD_READ, cid, nsid, slba, nlb, prp1, prp2, ring);
}

/**
 * NVMe submit a write command.
 * @param   ioq         io queue
 * @param   cid         command id
 * @param   nsid        namespace
 * @param   slba        startling logical block address
 * @param   nlb         number of blocks
 * @param   prp1        PRP1 address
 * @param   prp2        PRP2 address
 * @return  0 if ok else -1.
 */
int nvme_cmd_write(nvme_queue_t* ioq, u16 cid, int nsid,
                   u64 slba, int nlb, u64 prp1, u64 prp2, bool ring=true)
{
    return nvme_cmd_rw(ioq, NVME_CMD_WRITE, cid, nsid, slba, nlb, prp1, prp2, ring);
}

/**
 * Create an IO submission-completion queue pair.
 * @param   dev         device context
 * @param   ioq         if NULL then allocate queue
 * @param   id          queue id
 * @param   qsize       queue size
 * @param   sqbuf       submission queue buffer
 * @param   sqpa        submission queue IO physical address
 * @param   cqbuf       completion queue buffer
 * @param   cqpa        admin completion IO physical address
 * @return  pointer to the created io queue or NULL if failure.
 */
nvme_queue_t* nvme_ioq_create(nvme_device_t* dev, nvme_queue_t* ioq,
            int id, int qsize, void* sqbuf, u64 sqpa, void* cqbuf, u64 cqpa)
{
   if (!ioq) ioq = (nvme_queue_t*)zalloc(sizeof(*ioq));
    else ioq->ext = 1;

    ioq->dev = dev;
    ioq->id = id;
    ioq->size = qsize;
    ioq->sq = (nvme_sq_entry_t*)sqbuf;
    ioq->cq = (nvme_cq_entry_t*)cqbuf;
    ioq->sq_doorbell = dev->reg->sq0tdbl + (2 * id * dev->dbstride);
    //ioq->cq_doorbell = (std::atomic<u32>*) (ioq->sq_doorbell + dev->dbstride);
    ioq->cq_doorbell = (u32*) ioq->sq_doorbell + dev->dbstride;
    ioq->last_cq_doorbelled = qsize - 1;

    if (nvme_acmd_create_cq(ioq, cqpa) || nvme_acmd_create_sq(ioq, sqpa)) {
        free(ioq);
        return NULL;
    }
    return ioq;
}

/**
 * Delete an IO submission-completion queue pair.
 * @param   ioq         io queue to delete
 * @return  0 if ok else -1.
 */
int nvme_ioq_delete(nvme_queue_t* ioq)
{
    if (!ioq) return -1;
    if (nvme_acmd_delete_sq(ioq) || nvme_acmd_delete_cq(ioq)) return -1;
    if (!ioq->ext) free(ioq);
    return 0;
}

/**
 * NVMe setup admin submission-completion queue pair.
 * @param   dev         device context
 * @param   qsize       queue size
 * @param   sqbuf       submission queue buffer
 * @param   sqpa        submission queue IO physical address
 * @param   cqbuf       completion queue buffer
 * @param   cqpa        admin completion physical address
 * @return  pointer to the admin queue or NULL if failure.
 */
nvme_queue_t* nvme_adminq_setup(nvme_device_t* dev, int qsize,
                                void* sqbuf, u64 sqpa, void* cqbuf, u64 cqpa)
{
    if (nvme_ctlr_disable(dev)) return NULL;

    nvme_queue_t* adminq = &dev->adminq;
    adminq->dev = dev;
    adminq->id = 0;
    adminq->size = qsize;
    adminq->sq = (nvme_sq_entry_t*)sqbuf;
    adminq->cq = (nvme_cq_entry_t*)cqbuf;
    adminq->sq_doorbell = dev->reg->sq0tdbl;
    //adminq->cq_doorbell = (std::atomic<u32>*) (adminq->sq_doorbell + dev->dbstride);
    adminq->cq_doorbell = (u32*) adminq->sq_doorbell + dev->dbstride;

    nvme_adminq_attr_t aqa;
    aqa.val = 0;
    aqa.asqs = aqa.acqs = qsize - 1;
    w32(dev, &dev->reg->aqa.val, aqa.val);
    w64(dev, &dev->reg->asq, sqpa);
    w64(dev, &dev->reg->acq, cqpa);

    nvme_controller_config_t cc;
    cc.val = 0;
    cc.shn = 0;
    cc.ams = 0;
    cc.css = 0;
    cc.iosqes = 6;
    cc.iocqes = 4;
    cc.mps = dev->pageshift - 12;
    if (nvme_ctlr_enable(dev, cc)) return NULL;

    DEBUG_FN("qsize=%d cc=%#x aqa=%#x asq=%#lx acq=%#lx",
             qsize, cc.val, aqa.val, sqpa, cqpa);
    DEBUG_FN("vs=%#x intms=%#x intmc=%#x csts=%#x", dev->reg->vs.val,
             dev->reg->intms, dev->reg->intmc, dev->reg->csts.val);
    return adminq;
}

/**
 * Create an NVMe device context and map the controller register.
 * @param   dev         if NULL then allocate context
 * @param   mapfd       file descriptor for register mapping
 * @return  device context or NULL if failure.
 */
nvme_device_t* nvme_create(nvme_device_t* dev)
{
   if (!dev) dev = (nvme_device_t*)zalloc(sizeof(*dev));
    else dev->ext = 1;
    dev->rdtsec = rdtsc_second();

    dev->reg = globalReg;

    nvme_controller_cap_t cap;
    cap.val = r64(dev, &dev->reg->cap.val);
    dev->timeout = cap.to;              // in 500ms units
    dev->mpsmin = cap.mpsmin;           // 2 ^ (12 + MPSMIN)
    dev->mpsmax = cap.mpsmax;           // 2 ^ (12 + MPSMAX)
    dev->pageshift = 12 + dev->mpsmin;
    dev->maxqsize = cap.mqes + 1;
    dev->dbstride = 1 << cap.dstrd;     // in u32 size offset

    dev->cmbloc.val = r32(dev, &dev->reg->cmbloc.val);
    dev->cmbsz.val = r32(dev, &dev->reg->cmbsz.val);

    DEBUG_FN("cap=%#lx mps=%u-%u to=%u maxqs=%u dbs=%u cmbloc=%#x cmbsz=%#x",
             cap.val, cap.mpsmin, cap.mpsmax, cap.to, dev->maxqsize,
             dev->dbstride, dev->cmbloc.val, dev->cmbsz.val);

    return dev;
}

/**
 * Delete an NVMe device context
 * @param   dev         device context
 */
void nvme_delete(nvme_device_t* dev)
{
    if (dev && dev->reg) {
        if (munmap(dev->reg, sizeof(nvme_controller_reg_t))) {
            ERROR("munmap: %s", strerror(errno));
        }
    }
    if (!dev->ext) free(dev);
}



// Global static variables
static const char*      unvme_log = "/dev/shm/unvme.log";   ///< Log filename
static unvme_session_t* unvme_ses = NULL;                   ///< session list
static unvme_lock_t     unvme_lock = 0;                     ///< session lock


/**
 * Get a descriptor entry by moving from the free to the use list.
 * @param   q       queue
 * @return  the descriptor added to the use list.
 */
static unvme_desc_t* unvme_desc_get(unvme_queue_t* q)
{
    static u32 id = 0;
    unvme_desc_t* desc;

    if (q->descfree) {
        desc = q->descfree;
        LIST_DEL(q->descfree, desc);

        desc->error = 0;
        desc->cidcount = 0;
        u64* cidmask = desc->cidmask;
        int i = q->masksize >> 3;
        while (i--) *cidmask++ = 0;
    } else {
       desc = (unvme_desc_t*)zalloc(sizeof(unvme_desc_t) + q->masksize);
        desc->id = ++id;
        desc->q = q;
    }
    LIST_ADD(q->desclist, desc);
    if (desc == desc->next) q->descpend = desc; // head of pending list
    q->desccount++;
    return desc;
}

/**
 * Put a descriptor entry back by moving it from the use to the free list.
 * @param   desc    descriptor
 */
static void unvme_desc_put(unvme_desc_t* desc)
{
    unvme_queue_t* q = desc->q;

    // check to change the pending head or clear the list
    if (desc == q->descpend) {
        if (desc != desc->next) q->descpend = desc->next;
        else q->descpend = NULL;
    }

    LIST_DEL(q->desclist, desc);
    LIST_ADD(q->descfree, desc);
    q->desccount--;
}

/**
 * Process an I/O completion.
 * @param   q           queue
 * @param   timeout     timeout in seconds
 * @param   cqe_cs      CQE command specific DW0 returned
 * @return  0 if ok else NVMe error code (-1 means timeout).
 */
static int unvme_check_completion(unvme_queue_t* q, int timeout, u32* cqe_cs, bool writing=false)
{
    // wait for completion
    int err, cid;
    u64 endtsc = 0;
    bool broken = false;
    do {
        cid = nvme_check_completion(q->nvmeq, &err, cqe_cs);
        if (timeout == 0 || cid >= 0) {broken = true; break;}
        if (endtsc){} //sched_yield();
        else endtsc = processor::rdtsc() + timeout * q->nvmeq->dev->rdtsec;
    } while (processor::rdtsc() < endtsc);

    if (cid < 0){
        if(!broken){
            printf("timeout\n");
        }
        ucache::assert_crash(false);
        return cid;
    }

    // find the pending cid in the descriptor list to clear it
    unvme_desc_t* desc = q->descpend;
    int b = cid >> 6;
    u64 mask = (u64)1 << (cid & 63);
    while ((desc->cidmask[b] & mask) == 0) {
        desc = desc->next;
        if (desc == q->descpend)
            FATAL("pending cid %d not found", cid);
    }
    if (err) desc->error = err;

    if(ucache::uCacheManager != NULL){
        ucache::VMA* vma = ucache::uCacheManager->getVMA(desc->buf);
        if(vma != NULL){
            ucache::Buffer* buffer = vma->getBuffer(desc->buf); // TODO: make it depend on the actual lb size
            assert(buffer->clearIO(writing));
        }
    }

    // clear cid bit used
    desc->cidmask[b] &= ~mask;
    desc->cidcount--;
    q->cidmask[b] &= ~mask;
    q->cidcount--;
    q->cid = cid;

    // check to advance next pending descriptor
    if (q->cidcount) {
        while (q->descpend->cidcount == 0) q->descpend = q->descpend->next;
    }
    PDEBUG("# c q%d={%d %d %#lx} d={%d %d %#lx} @%d",
           q->nvmeq->id, cid, q->cidcount, *q->cidmask,
           desc->id, desc->cidcount, *desc->cidmask, q->descpend->id);
    return err;
}

int unvme_check_completion(const unvme_ns_t* ns, int qid, int timeout){
    unvme_queue_t* q = ((unvme_session_t*)ns->ses)->dev->ioqs + qid;
    return unvme_check_completion(q, timeout, NULL);
}

/**
 * Get a free cid.  If queue is full then process currently pending submissions.
 * @param   desc        descriptor
 * @return  cid.
 */
static u16 unvme_get_cid(unvme_desc_t* desc)
{
    u16 cid;
    unvme_queue_t* q = desc->q;
    int qsize = q->size;

    // if submission queue is full then process completion first
    if ((q->cidcount + 1) == qsize) {
        int err = unvme_check_completion(q, UNVME_TIMEOUT, NULL);
        if (err) {
            if (err == -1) FATAL("q%d timeout", q->nvmeq->id);
            else ERROR("q%d error %#x", q->nvmeq->id, err);
        }
    }

    // get a free cid
    cid = q->cid;
    if(q->cidmask == NULL){
        printf("cidmask null for q : %u\n", q->nvmeq->id);
        assert(false);
    }
    while (q->cidmask[cid >> 6] & ((u64)1L << (cid & 63))) {
        if (++cid >= qsize) cid = 0;
    }

    // set cid bit used
    int b = cid >> 6;
    u64 mask = (u64)1 << (cid & 63);
    desc->cidmask[b] |= mask;
    desc->cidcount++;
    q->cidmask[b] |= mask;
    q->cidcount++;
    q->cid = cid;
    if (++q->cid >= qsize) q->cid = 0;

    return cid;
}

/**
 * Lookup DMA address associated with the user buffer.
 * @param   ns          namespace handle
 * @param   buf         user data buffer
 * @param   bufsz       buffer size
 * @return  DMA address or -1L if error.
 */
static u64 unvme_map_dma(const unvme_ns_t* ns, void* buf, u64 bufsz)
{
#ifdef UNVME_IDENTITY_MAP_DMA
    //u64 addr = (u64)buf & dev->vfiodev.iovamask;
    //TODO: this doesn't work for malloc-ed memory
    // need to use this instead u64 addr = mmu::virt_to_phys_dynamic_phys(buf);
    //u64 addr = mmu::virt_to_phys_dynamic_phys(buf);
    u64 addr = ucache::walk(buf).phys<<12;
    assert(addr!=0);
#else
    unvme_device_t* dev = ((unvme_session_t*)ns->ses)->dev;
    vfio_dma_t* dma = NULL;
    unvme_lockr(&dev->iomem.lock);
    int i;
    for (i = 0; i < dev->iomem.count; i++) {
        dma = dev->iomem.map[i];
        if (dma->buf <= buf && (char*)buf < (char*)((char*)dma->buf + dma->size)) break;
    }
    unvme_unlockr(&dev->iomem.lock);
    if (i == dev->iomem.count)
        FATAL("invalid I/O buffer address");
    u64 addr = dma->addr + (u64)((char*)buf - (char*)dma->buf);
    if ((addr + bufsz) > (dma->addr + dma->size))
        FATAL("buffer overrun");
#endif
    //if ((addr & (ns->blocksize - 1)) != 0)
    //    FATAL("unaligned buffer address");
    return addr;
}

/**
 * Map the user buffer to PRP addresses (compose PRP list as necessary).
 * @param   ns          namespace handle
 * @param   q           queue
 * @param   cid         queue entry index
 * @param   buf         user buffer
 * @param   bufsz       buffer size
 * @param   prp1        returned prp1 value
 * @param   prp2        returned prp2 value
 * @return  0 if ok else -1 if buffer address error.
 */
static int unvme_map_prps(const unvme_ns_t* ns, unvme_queue_t* q, int cid,
                          void* buf, u64 bufsz, u64* prp1, u64* prp2)
{
    u64 addr = unvme_map_dma(ns, buf, bufsz);
    if (addr == -1ULL) return -1;

    *prp1 = addr;
    *prp2 = 0;
    int numpages = (bufsz + ns->pagesize - 1) >> ns->pageshift;
    if (numpages == 2) {
        *prp2 = addr + ns->pagesize;
    } else if (numpages > 2) {
        int prpoff = cid << ns->pageshift;
        u64* prplist = (u64*)((char*)q->prplist->buf + prpoff);
        *prp2 = q->prplist->addr + prpoff;
        int i;
        for (i = 1; i < numpages; i++) {
            addr += ns->pagesize;
            *prplist++ = addr;
        }
    }
    return 0;
}

/**
 * Submit a generic (vendor specific) NVMe command.
 * @param   ns          namespace handle
 * @param   desc        descriptor
 * @param   buf         data buffer
 * @param   slba        starting lba
 * @param   nlb         number of logical blocks
 * @return  cid if ok else -1.
 */
static int unvme_submit_io(const unvme_ns_t* ns, unvme_desc_t* desc,
                           void* buf, u64 slba, u32 nlb, bool ring=true)
{
    u64 prp1, prp2;
    unvme_queue_t* ioq = desc->q;
    u16 cid = unvme_get_cid(desc);
    u64 bufsz = (u64)nlb << ns->blockshift;
    if (unvme_map_prps(ns, ioq, cid, buf, bufsz, &prp1, &prp2)) return -1;

    // submit I/O command
    if (nvme_cmd_rw(ioq->nvmeq, desc->opc, cid,
                    ns->id, slba, nlb, prp1, prp2, ring)) return -1;
    PDEBUG("# %c %#lx %#x q%d={%d %d %#lx} d={%d %d %#lx}",
           desc->opc == NVME_CMD_READ ? 'r' : 'w', slba, nlb,
           ioq->nvmeq->id, cid, ioq->cidcount, *ioq->cidmask,
           desc->id, desc->cidcount, *desc->cidmask);
    return cid;
}

/**
 * Initialize a queue allocating descriptors and PRP list pages.
 * @param   dev         device context
 * @param   q           queue
 * @param   qsize       queue depth
 */
static void unvme_queue_init(unvme_device_t* dev, unvme_queue_t* q, int qsize)
{
    memset(q, 0, sizeof(*q));
    q->size = qsize;

    // allocate queue entries and PRP list
    q->sqdma = vfio_dma_alloc(qsize * sizeof(nvme_sq_entry_t));
    q->cqdma = vfio_dma_alloc(qsize * sizeof(nvme_cq_entry_t));
    q->prplist = vfio_dma_alloc(qsize << dev->ns.pageshift);
    if (!q->sqdma || !q->cqdma || !q->prplist)
        FATAL("vfio_dma_alloc");

    // setup descriptors and pending masks
    q->masksize = ((qsize + 63) >> 6) << 3; // (qsize + 63) / 64) * sizeof(u64)
    q->cidmask = (u64*)zalloc(q->masksize);
    int i;
    for (i = 0; i < 16; i++) unvme_desc_get(q);
    q->descfree = q->desclist;
    q->desclist = NULL;
    q->desccount = 0;
}

/**
 * Clean up a queue freeing its memory allocation.
 * @param   q           queue
 */
static void unvme_queue_cleanup(unvme_queue_t* q)
{
    // free all descriptors
    unvme_desc_t* desc;
    while ((desc = q->desclist) != NULL) {
        LIST_DEL(q->desclist, desc);
        free(desc);
    }
    while ((desc = q->descfree) != NULL) {
        LIST_DEL(q->descfree, desc);
        free(desc);
    }

    if (q->cidmask) free(q->cidmask);
    if (q->prplist) vfio_dma_free(q->prplist);
    if (q->cqdma) vfio_dma_free(q->cqdma);
    if (q->sqdma) vfio_dma_free(q->sqdma);
}

/**
 * Setup admin queue.
 * @param   dev         device context
 * @param   qsize       admin queue depth
 */
static void unvme_adminq_create(unvme_device_t* dev, int qsize)
{
    //DEBUG_FN("%x", dev->vfiodev.pci);
    unvme_queue_t* adminq = &dev->adminq;
    unvme_queue_init(dev, adminq, qsize);
    if (!nvme_adminq_setup(&dev->nvmedev, qsize,
                           adminq->sqdma->buf, adminq->sqdma->addr,
                           adminq->cqdma->buf, adminq->cqdma->addr))
        FATAL("nvme_setup_adminq failed");
    adminq->nvmeq = &dev->nvmedev.adminq;
}

/**
 * Delete admin queue.
 * @param   dev         device context
 */
static void unvme_adminq_delete(unvme_device_t* dev)
{
    //DEBUG_FN("%x", dev->vfiodev.pci);
    unvme_queue_cleanup(&dev->adminq);
}

/**
 * Create an I/O queue.
 * @param   dev         device context
 * @param   q           queue index starting at 0
 */
static void unvme_ioq_create(unvme_device_t* dev, int q)
{
    //DEBUG_FN("%x q=%d", dev->vfiodev.pci, q+1);
    unvme_queue_t* ioq = dev->ioqs + q;
    unvme_queue_init(dev, ioq, dev->ns.qsize);
    if (!(ioq->nvmeq = nvme_ioq_create(&dev->nvmedev, NULL, q+1, ioq->size,
                                       ioq->sqdma->buf, ioq->sqdma->addr,
                                       ioq->cqdma->buf, ioq->cqdma->addr)))
        FATAL("nvme_ioq_create %d failed", q+1);
    //DEBUG_FN("%x q=%d qd=%d db=%#04lx", dev->vfiodev.pci, ioq->nvmeq->id,
    //        ioq->size, (u64)ioq->nvmeq->sq_doorbell - (u64)dev->nvmedev.reg);
}

/**
 * Delete an I/O queue.
 * @param   dev         device context
 * @param   q           queue index starting at 0
 */
static void unvme_ioq_delete(unvme_device_t* dev, int q)
{
    //DEBUG_FN("%x %d", dev->vfiodev.pci, q+1);
    unvme_queue_t* ioq = dev->ioqs + q;
    (void)nvme_ioq_delete(ioq->nvmeq);
    unvme_queue_cleanup(ioq);
}

/**
 * Initialize a namespace instance.
 * @param   ns          namespace context
 * @param   nsid        namespace id
 */
static void unvme_ns_init(unvme_ns_t* ns, int nsid)
{
    unvme_device_t* dev = ((unvme_session_t*)ns->ses)->dev;
    ns->id = nsid;
    ns->maxiopq = ns->qsize - 1;

    vfio_dma_t* dma = vfio_dma_alloc(ns->pagesize);
    if (nvme_acmd_identify(&dev->nvmedev, nsid, dma->addr, 0))
        FATAL("nvme_acmd_identify %d failed", nsid);
    nvme_identify_ns_t* idns = (nvme_identify_ns_t*)dma->buf;
    ns->blockcount = idns->ncap;
    ns->blockshift = idns->lbaf[idns->flbas & 0xF].lbads;
    ns->blocksize = 1 << ns->blockshift;
    ns->bpshift = ns->pageshift - ns->blockshift;
    ns->nbpp = 1 << ns->bpshift;
    ns->pagecount = ns->blockcount >> ns->bpshift;
    ns->maxbpio = ns->maxppio << ns->bpshift;
    vfio_dma_free(dma);

    //sprintf(ns->device + strlen(ns->device), "/%d", nsid);
    /*DEBUG_FN("%s qc=%d qd=%d bs=%d bc=%#lx mbio=%d", ns->device, ns->qcount,
             ns->qsize, ns->blocksize, ns->blockcount, ns->maxbpio);*/
}

/**
 * Clean up.
 */
static void unvme_cleanup(unvme_session_t* ses)
{
    unvme_device_t* dev = ses->dev;
    if (--dev->refcount == 0) {
        //DEBUG_FN("%s", ses->ns.device);
        int q;
        for (q = 0; q < (int)dev->ns.qcount; q++) unvme_ioq_delete(dev, q);
        unvme_adminq_delete(dev);
        nvme_delete(&dev->nvmedev);
        free(dev->ioqs);
        free(dev);
    }
    LIST_DEL(unvme_ses, ses);
    free(ses);
    if (!unvme_ses) log_close();
}


/**
 * Open and attach to a UNVMe driver.
 * @param   pci         PCI device id
 * @param   nsid        namespace id
 * @param   qcount      number of queues (0 for max number of queues support)
 * @param   qsize       size of each queue (0 default to 65)
 * @return  namespace pointer or NULL if error.
 */
unvme_ns_t* unvme_do_open(int nsid, int qcount, int qsize)
{
    unvme_lockw(&unvme_lock);
    if (!unvme_ses) {
        if (log_open(unvme_log, "w")) {
            unvme_unlockw(&unvme_lock);
            exit(1);
        }
    }

    // check for existing opened device
    unvme_session_t* xses = unvme_ses;
    while (xses) {
       /*if (xses->ns.pci == pci) {
            if (nsid > xses->ns.nscount) {
                ERROR("invalid %06x nsid %d (max %d)", pci, nsid, xses->ns.nscount);
                return NULL;
            }
            if (xses->ns.id == nsid) {
                ERROR("%06x nsid %d is in use", pci);
                return NULL;
            }
            break;
            }*/
        xses = xses->next;
        if (xses == unvme_ses) xses = NULL;
    }

    unvme_device_t* dev;
    if (xses) {
        dev = xses->dev;
    } else {
        // setup controller namespace
        dev = (unvme_device_t*)zalloc(sizeof(unvme_device_t));
        nvme_create(&dev->nvmedev);
        unvme_adminq_create(dev, 64);

        // get controller info
        vfio_dma_t* dma = vfio_dma_alloc(4096);
        if (nvme_acmd_identify(&dev->nvmedev, 0, dma->addr, 0))
            FATAL("nvme_acmd_identify controller failed");
        nvme_identify_ctlr_t* idc = (nvme_identify_ctlr_t*)dma->buf;
        if (nsid > (int)idc->nn) {
           //ERROR("invalid %06x nsid %d (max %d)", pci, nsid, idc->nn);
            return NULL;
        }

        unvme_ns_t* ns = &dev->ns;
        ns->id = 0;
        ns->nscount = idc->nn;
        //sprintf(ns->device, "%02x:%02x.%x", pci >> 16, (pci >> 8) & 0xff, pci & 0xff);
        ns->maxqsize = dev->nvmedev.maxqsize;
        ns->pageshift = dev->nvmedev.pageshift;
        ns->pagesize = 1 << ns->pageshift;
        int i;
        ns->vid = idc->vid;
        memcpy(ns->mn, idc->mn, sizeof (ns->mn));
        for (i = sizeof (ns->mn) - 1; i > 0 && ns->mn[i] == ' '; i--) ns->mn[i] = 0;
        memcpy(ns->sn, idc->sn, sizeof (ns->sn));
        for (i = sizeof (ns->sn) - 1; i > 0 && ns->sn[i] == ' '; i--) ns->sn[i] = 0;
        memcpy(ns->fr, idc->fr, sizeof (ns->fr));
        for (i = sizeof (ns->fr) - 1; i > 0 && ns->fr[i] == ' '; i--) ns->fr[i] = 0;

        // set limit to 1 PRP list page per IO submission
        ns->maxppio = ns->pagesize / sizeof(u64);
        if (idc->mdts) {
            int mp = 2 << (idc->mdts - 1);
            if (ns->maxppio > mp) ns->maxppio = mp;
        }
        vfio_dma_free(dma);

        // get max number of queues supported
        nvme_feature_num_queues_t nq;
        if (nvme_acmd_get_features(&dev->nvmedev, 0,
                                   NVME_FEATURE_NUM_QUEUES, 0, 0, (u32*)&nq))
            FATAL("nvme_acmd_get_features number of queues failed");
        int maxqcount = (nq.nsq < nq.ncq ? nq.nsq : nq.ncq) + 1;
        if (qcount <= 0) qcount = maxqcount;
        if (qsize <= 1) qsize = UNVME_QSIZE;
        if (qsize > (int)dev->nvmedev.maxqsize) qsize = dev->nvmedev.maxqsize;
        ns->maxqcount = maxqcount;
        ns->qcount = qcount;
        ns->qsize = qsize;

        // setup IO queues
        dev->ioqs = (unvme_queue_t*)zalloc(qcount * sizeof(unvme_queue_t));
        //last_cq_doorbelled.reserve(1+qcount);
        for (i = 0; i < qcount; i++){
            unvme_ioq_create(dev, i);
            //last_cq_doorbelled[i+1]=qsize-1;
        }
    }

    // allocate new session
    unvme_session_t* ses = (unvme_session_t*)zalloc(sizeof(unvme_session_t));
    ses->dev = dev;
    dev->refcount++;
    memcpy(&ses->ns, &ses->dev->ns, sizeof(unvme_ns_t));
    ses->ns.ses = ses;
    unvme_ns_init(&ses->ns, nsid);
    LIST_ADD(unvme_ses, ses);

    //INFO_FN("%s (%.40s) is ready", ses->ns.device, ses->ns.mn);
    unvme_unlockw(&unvme_lock);
    return &ses->ns;
}

/**
 * Close and detach from a UNVMe driver.
 * @param   ns          namespace handle
 * @return  0 if ok else -1.
 */
int unvme_do_close(const unvme_ns_t* ns)
{
    //DEBUG_FN("%s", ns->ses->device);
    unvme_session_t* ses = (unvme_session_t*)ns->ses;
    //if (ns->pci != ses->dev->vfiodev.pci) return -1;
    unvme_lockw(&unvme_lock);
    unvme_cleanup(ses);
    unvme_unlockw(&unvme_lock);
    return 0;
}

/**
 * Allocate an I/O buffer.
 * @param   ns          namespace handle
 * @param   size        buffer size
 * @return  the allocated buffer or NULL if failure.
 */
void* unvme_do_alloc(const unvme_ns_t* ns, u64 size)
{
    //DEBUG_FN("%s %#lx", ns->ses->dev, size);
    unvme_device_t* dev = ((unvme_session_t*)ns->ses)->dev;
    unvme_iomem_t* iomem = &dev->iomem;
    void* buf = NULL;

    unvme_lockw(&iomem->lock);
    vfio_dma_t* dma = vfio_dma_alloc(size);
    if (dma) {
        if (iomem->count == iomem->size) {
            iomem->size += 256;
            iomem->map = (vfio_dma_t**)realloc(iomem->map, iomem->size * sizeof(void*));
        }
        iomem->map[iomem->count++] = dma;
        buf = dma->buf;
    }
    unvme_unlockw(&iomem->lock);
    return buf;
}

/**
 * Free an I/O buffer.
 * @param   ns          namespace handle
 * @param   buf         buffer pointer
 * @return  0 if ok else -1.
 */
int unvme_do_free(const unvme_ns_t* ns, void* buf)
{
    //DEBUG_FN("%s %p", ns->ses->dev, buf);
    unvme_device_t* dev = ((unvme_session_t*)ns->ses)->dev;
    unvme_iomem_t* iomem = &dev->iomem;

    unvme_lockw(&iomem->lock);
    int i;
    for (i = 0; i < iomem->count; i++) {
        if (buf == iomem->map[i]->buf) {
            vfio_dma_free(iomem->map[i]);
            iomem->count--;
            if (i != iomem->count)
                iomem->map[i] = iomem->map[iomem->count];
            unvme_unlockw(&iomem->lock);
            return 0;
        }
    }
    unvme_unlockw(&iomem->lock);
    return -1;
}

/**
 * Poll for completion status of a previous IO submission.
 * If there's no error, the descriptor will be released.
 * @param   desc        IO descriptor
 * @param   timeout     in seconds
 * @param   cqe_cs      CQE command specific DW0 returned
 * @return  0 if ok else error status (-1 means timeout).
 */
int unvme_do_poll(unvme_desc_t* desc, int timeout, u32* cqe_cs)
{
    if (desc->sentinel != desc)
        FATAL("bad IO descriptor");

    PDEBUG("# POLL d={%d %d %#lx}", desc->id, desc->cidcount, *desc->cidmask);
    int err = 0;
    int cnt = -desc->cidcount;
    while (desc->cidcount) {
        bool writing = (desc->opc == NVME_CMD_WRITE);
        if ((err = unvme_check_completion(desc->q, timeout, cqe_cs, writing)) != 0){
            ucache::assert_crash(false);
            break;
        }
        cnt++;
    }
    ucache::uCacheManager->poll_depth += cnt;
    ucache::uCacheManager->poll_depth_count++;
    if (desc->cidcount == 0) unvme_desc_put(desc);
    PDEBUG("# q%d +%d", desc->q->nvmeq->id, desc->q->desccount);

    return err;
}

/**
 * Submit a read/write command that may require multiple I/O submissions
 * and processing some completions.
 * @param   ns          namespace handle
 * @param   qid         queue id
 * @param   opc         op code
 * @param   buf         data buffer
 * @param   slba        starting lba
 * @param   nlb         number of logical blocks
 * @return  I/O descriptor or NULL if error.
 */
unvme_desc_t* unvme_do_rw(const unvme_ns_t* ns, int qid, int opc,
                          void* buff, u64 slba, u32 nlb, bool ring=true)
{
   char* buf = (char*)buff;
    unvme_queue_t* q = ((unvme_session_t*)ns->ses)->dev->ioqs + qid;
    unvme_desc_t* desc = unvme_desc_get(q);
    desc->opc = opc;
    desc->buf = buf;
    desc->qid = qid;
    desc->slba = slba;
    desc->nlb = nlb;
    desc->sentinel = desc;

    PDEBUG("# %s %#lx %#x @%d +%d", opc == NVME_CMD_READ ? "READ" : "WRITE",
           slba, nlb, desc->id, q->desccount);
    while (nlb) {
        int n = ns->maxbpio;
        if (n > (int)nlb) n = nlb;
        int cid = unvme_submit_io(ns, desc, buf, slba, n, ring);
        if (cid < 0) {
            // poll currently pending descriptor
            int err = unvme_do_poll(desc, UNVME_SHORT_TIMEOUT, NULL);
            if (err) {
                if (err == -1) return NULL; //FATAL("q%d timeout", q->nvmeq->id);
                else ERROR("q%d error %#x", q->nvmeq->id, err);
            }
        }

        buf += n << ns->blockshift;
        slba += n;
        nlb -= n;
    }

    return desc;
}

/**
 * Submit a generic or vendor specific command.
 * @param   ns          namespace handle
 * @param   qid         client queue index (-1 for admin queue)
 * @param   opc         command op code
 * @param   nsid        namespace id
 * @param   cdw10_15    NVMe command word 10 through 15
 * @param   buf         data buffer (from unvme_alloc)
 * @param   bufsz       data buffer size
 * @return  command descriptor or NULL if error.
 */
unvme_desc_t* unvme_do_cmd(const unvme_ns_t* ns, int qid, int opc, int nsid,
                           void* buf, u64 bufsz, u32 cdw10_15[6])
{
    unvme_device_t* dev = ((unvme_session_t*)ns->ses)->dev;
    unvme_queue_t* q = (qid == -1) ? &dev->adminq : &dev->ioqs[qid];
    unvme_desc_t* desc = unvme_desc_get(q);
    desc->opc = opc;
    desc->buf = buf;
    desc->qid = qid;
    desc->sentinel = desc;

    u64 prp1, prp2;
    u16 cid = unvme_get_cid(desc);
    if (unvme_map_prps(ns, q, cid, buf, bufsz, &prp1, &prp2) ||
        nvme_cmd_vs(q->nvmeq, opc, cid, nsid, prp1, prp2, cdw10_15)) {
        unvme_desc_put(desc);
        return NULL;
    }

    PDEBUG("# CMD=%#x %d q%d={%d %d %#lx} d={%d %d %#lx}",
           opc, nsid, q->nvmeq->id, cid, q->cidcount, *q->cidmask,
           desc->id, desc->cidcount, *desc->cidmask);
    return desc;
}


//TRACEPOINT(trace_unvme_read, "size=%d, nlb=%u", int, u32); 

/**
 * Open a client session with specified number of IO queues and queue size.
 * @param   pciname     PCI device name (as %x:%x.%x[/NSID] format)
 * @param   qcount      number of io queues
 * @param   qsize       io queue size
 * @return  namespace pointer or NULL if error.
 */
const unvme_ns_t* unvme_openq(int qcount, int qsize)
{
    if (qcount < 0 || qsize < 0 || qsize == 1) {
        ERROR("invalid qcount %d or qsize %d", qcount, qsize);
        return NULL;
    }

    int nsid = 1;
    return unvme_do_open(nsid, qcount, qsize);
}

/**
 * Open a client session.
 * @param   pciname     PCI device name (as %x:%x.%x[/NSID] format)
 * @return  namespace pointer or NULL if error.
 */
const unvme_ns_t* unvme_open()
{
    return unvme_openq(0, 0);
}

/**
 * Close a client session and delete its contained io queues.
 * @param   ns          namespace handle
 * @return  0 if ok else error code.
 */
int unvme_close(const unvme_ns_t* ns)
{
    return unvme_do_close(ns);
}

/**
 * Allocate an I/O buffer associated with a session.
 * @param   ns          namespace handle
 * @param   size        buffer size
 * @return  the allocated buffer or NULL if failure.
 */
void* unvme_alloc(const unvme_ns_t* ns, u64 size)
{
    return unvme_do_alloc(ns, size);
}

/**
 * Free an I/O buffer associated with a session.
 * @param   ns          namespace handle
 * @param   buf         buffer pointer
 * @return  0 if ok else -1.
 */
int unvme_free(const unvme_ns_t* ns, void* buf)
{
    return unvme_do_free(ns, buf);
}

/**
 * Submit a generic or vendor specific command.
 * @param   ns          namespace handle
 * @param   qid         client queue index (-1 for admin queue)
 * @param   opc         command op code
 * @param   nsid        namespace id
 * @param   buf         data buffer (from unvme_alloc)
 * @param   bufsz       data buffer size
 * @param   cdw10_15    NVMe command word 10 through 15
 * @return  descriptor or NULL if failed.
 */
unvme_iod_t unvme_acmd(const unvme_ns_t* ns, int qid, int opc, int nsid,
                              void* buf, u64 bufsz, u32 cdw10_15[6])
{
    return (unvme_iod_t)unvme_do_cmd(ns, qid, opc, nsid, buf, bufsz, cdw10_15);
}

/**
 * Read data from specified logical blocks on device.
 * @param   ns          namespace handle
 * @param   qid         client queue index
 * @param   buf         data buffer (from unvme_alloc)
 * @param   slba        starting logical block
 * @param   nlb         number of logical blocks
 * @return  I/O descriptor or NULL if failed.
 */
unvme_iod_t unvme_aread(const unvme_ns_t* ns, int qid, void* buf, u64 slba, u32 nlb, bool ring)
{
    return (unvme_iod_t)unvme_do_rw(ns, qid, NVME_CMD_READ, buf, slba, nlb, ring);
}

/**
 * Write data to specified logical blocks on device.
 * @param   ns          namespace handle
 * @param   qid         client queue index
 * @param   buf         data buffer (from unvme_alloc)
 * @param   slba        starting logical block
 * @param   nlb         number of logical blocks
 * @return  I/O descriptor or NULL if failed.
 */
unvme_iod_t unvme_awrite(const unvme_ns_t* ns, int qid,
                         const void* buf, u64 slba, u32 nlb, bool ring)
{
    return (unvme_iod_t)unvme_do_rw(ns, qid, NVME_CMD_WRITE, (void*)buf, slba, nlb, ring);
} // this function used to be inline which caused problems of linking
  // TODO maybe put them (all the async func) in the header file directly ? 

/**
 * Poll for completion status of a previous IO submission.
 * If there's no error, the descriptor will be freed.
 * @param   iod         IO descriptor
 * @param   timeout     in seconds
 * @return  0 if ok else error status (-1 for timeout).
 */
int unvme_apoll(unvme_iod_t iod, int timeout)
{
    return unvme_do_poll((unvme_desc_t*)iod, timeout, NULL);
}

/**
 * Poll for completion status of a previous IO submission.
 * If there's no error, the descriptor will be freed.
 * @param   iod         IO descriptor
 * @param   timeout     in seconds
 * @param   cqe_cs      CQE command specific DW0 returned
 * @return  0 if ok else error status (-1 for timeout).
 */
int unvme_apoll_cs(unvme_iod_t iod, int timeout, u32* cqe_cs)
{
    return unvme_do_poll((unvme_desc_t*)iod, timeout, cqe_cs);
}

/**
 * Submit a generic or vendor specific command and then poll for completion.
 * @param   ns          namespace handle
 * @param   qid         client queue index (-1 for admin queue)
 * @param   opc         command op code
 * @param   nsid        namespace id
 * @param   buf         data buffer (from unvme_alloc)
 * @param   bufsz       data buffer size
 * @param   cdw10_15    NVMe command word 10 through 15
 * @param   cqe_cs      CQE command specific DW0 returned
 * @return  descriptor or NULL if failed.
 */
int unvme_cmd(const unvme_ns_t* ns, int qid, int opc, int nsid,
              void* buf, u64 bufsz, u32 cdw10_15[6], u32* cqe_cs)
{
    unvme_iod_t iod = unvme_acmd(ns, qid, opc, nsid, buf, bufsz, cdw10_15);
    if (iod) {
        sched_yield();
        return unvme_apoll_cs(iod, UNVME_SHORT_TIMEOUT, cqe_cs);
    }
    return -1;
}

/**
 * Read data from specified logical blocks on device.
 * @param   ns          namespace handle
 * @param   qid         client queue index
 * @param   buf         data buffer (from unvme_alloc)
 * @param   slba        starting logical block
 * @param   nlb         number of logical blocks
 * @return  0 if ok else error status.
 */
int unvme_read(const unvme_ns_t* ns, int qid, void* buf, u64 slba, u32 nlb)
{
    //trace_unvme_read(qid, nlb);
    unvme_iod_t iod = unvme_aread(ns, qid, buf, slba, nlb);
    if (iod) {
        //sched_yield();
        return unvme_apoll(iod, UNVME_SHORT_TIMEOUT);
    }
    ucache::assert_crash(false);
    return -1;
}

/**
 * Write data to specified logical blocks on device.
 * @param   ns          namespace handle
 * @param   qid         client queue index
 * @param   buf         data buffer (from unvme_alloc)
 * @param   slba        starting logical block
 * @param   nlb         number of logical blocks
 * @return  0 if ok else error status.
 */
int unvme_write(const unvme_ns_t* ns, int qid,
                const void* buf, u64 slba, u32 nlb)
{
    unvme_iod_t iod = unvme_awrite(ns, qid, buf, slba, nlb);
    if (iod) {
        sched_yield();
        return unvme_apoll(iod, UNVME_SHORT_TIMEOUT);
    }
    return -1;
}


using namespace memory;

#define nvme_tag "nvme"
#define nvme_d(...)   tprintf_d(nvme_tag, __VA_ARGS__)
#define nvme_i(...)   tprintf_i(nvme_tag, __VA_ARGS__)
#define nvme_w(...)   tprintf_w(nvme_tag, __VA_ARGS__)
#define nvme_e(...)   tprintf_e(nvme_tag, __VA_ARGS__)


vfio_dma_t* vfio_dma_alloc(size_t size)
{
    vfio_dma_t* p = (vfio_dma_t*)malloc(sizeof(vfio_dma_t));
    size = align_up(size, mmu::page_size);
    p->size = size;
    p->buf = (void*) memory::alloc_phys_contiguous_aligned(size, mmu::page_size);
    memset(p->buf, 0, size);
    p->addr = mmu::virt_to_phys(p->buf);
    /*if (size <= 4096) {
        p->buf = (void*)mmap(NULL, size, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS, -1, 0);
        madvise(p->buf, size, MADV_NOHUGEPAGE);
        memset(p->buf, 0, size);
        //p->addr = mmu::virt_to_phys(p->buf);
        p->addr = ucache::walk(p->buf).phys << 12;
    } else {
        assert(size <= 2*1024*1024);
        p->buf = (void*)mmap(NULL, 2*1024*1024, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS, -1, 0);
        madvise(p->buf, 2*1024*1024, MADV_HUGEPAGE);
        memset(p->buf, 0, 2*1024*1024);
        //p->addr = mmu::virt_to_phys(p->buf);
        p->addr = ucache::walkHuge(p->buf).phys <<12;
    }*/
    //std::cout << std::bitset<64>(p->addr) << std::endl;
    assert(p->buf);
    return p;
}

int vfio_dma_free(vfio_dma_t* dma)
{
   return 0;
}

static inline double gettime(void) {
  struct timeval now_tv;
  gettimeofday (&now_tv, NULL);
  return ((double)now_tv.tv_sec) + ((double)now_tv.tv_usec)/1000000.0;
}

static int nvme_read(struct device *dev, struct uio *uio, int io_flags){
    return bdev_read(dev, uio, io_flags);
}

static int nvme_write(struct device *dev, struct uio *uio, int io_flags){
    return bdev_write(dev, uio, io_flags);
}

static int nvme_open(struct device* dev, int io_flags){
    return 0;
}

static struct devops nvme_devops {
    nvme_open,
    no_close,
    nvme_read,
    nvme_write,
    no_ioctl,
    no_devctl
};

struct driver nvme_driver {
    "nvme",
    &nvme_devops
};

nvme::nvme(pci::device &dev)
    : _dev(dev)
{
   parse_pci_config();
   globalReg = (nvme_controller_reg_t*)_bar0->get_mmio();

   u16 command = dev.get_command();
   command |= 0x4 | 0x2 | 0x400;
   dev.set_command(command);

    ns = unvme_openq(sched::max_cpus, ucache::maxQueueSize);
    while(!ns){ ns = unvme_openq(sched::max_cpus, ucache::maxQueueSize); };
    //printf("model: '%.40s' sn: '%.20s' fr: '%.8s' ", ns->mn, ns->sn, ns->fr);
    //printf("page size = %d, queue count = %d/%d (max queue count), queue size = %d/%d (max queue size), block count = %#lx, block size = %d, max block io = %d\n", ns->pagesize, ns->qcount, ns->maxqcount, ns->qsize, ns->maxqsize, ns->blockcount, ns->blocksize, ns->maxbpio);
    
    std::string dev_name("nvme");
    dev_name += std::to_string(__disk_idx);
    struct device* osv_dev = device_create(&nvme_driver, dev_name.c_str(), D_BLK);
    read_partition_table(osv_dev);

    printf("model: '%.40s', size : %lu GiB, blockcount: %lu, block size = %d, with %lu queues\n", ns->mn, (ns->blockcount*ns->blocksize)/(1024ull*1024*1024), ns->blockcount, ns->blocksize, ns->qcount);
}

void nvme::dump_config(void)
{
   u8 B, D, F;
   _dev.get_bdf(B, D, F);

   _dev.dump_config();
   nvme_d("%s [%x:%x.%x] vid:id= %x:%x", get_name().c_str(),
          (u16)B, (u16)D, (u16)F,
          _dev.get_vendor_id(),
          _dev.get_device_id());
}

void nvme::parse_pci_config()
{
   _bar0 = _dev.get_bar(1);
   _bar0->map();
   if (_bar0 == nullptr) {
      throw std::runtime_error("BAR1 is absent");
   }
   if(!_bar0->is_mapped()){
       throw std::runtime_error("BAR1 is not mapped");
   }
   assert(_bar0->is_mapped());
}

hw_driver* nvme::probe(hw_device* dev)
{
   if (auto pci_dev = dynamic_cast<pci::device*>(dev)) {
      if ((pci_dev->get_base_class_code()==1) && (pci_dev->get_sub_class_code()==8) && (pci_dev->get_programming_interface()==2)){ // detect NVMe device
         nvme* device = aligned_new<nvme>(*pci_dev);
         if(ucache::uCacheManager == NULL)
             ucache::uCacheManager = new ucache::uCache();
         ucache::uCacheManager->fs->add_device(device->ns);
         return device;
      }
   }
   return nullptr;
}
