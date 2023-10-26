static int nvme_ctlr_wait_ready(nvme_device_t* dev, int ready)
{
    nvme_controller_cap_t cap;
    cap.val = dev->reg->cap.val;
    int timeout = cap.to; // in 500ms unit

    int i;
    for (i = 0; i < timeout; i++) {
        usleep(500000);
        nvme_controller_status_t csts;
        csts.val = dev->reg->csts.val;
        if (csts.rdy == ready) return 0;
    }

    std::cout << "timeout in nvme_ctlr_wait_ready " << dev->reg->csts.rdy << " " << ready << " " << dev->reg->csts.cfs<< std::endl;
    return -1;
}

/**
 * Disable controller.
 * @param   dev         device context
 * @return  0 if ok else -1.
 */
static int nvme_ctlr_disable(nvme_device_t* dev)
{
    nvme_controller_config_t cc;
    cc.val = dev->reg->cc.val;
    cc.en = 0;
    dev->reg->cc.val = cc.val;
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
    cc.en = 1;
    dev->reg->cc.val = cc.val;
    return nvme_ctlr_wait_ready(dev, 1);
}

nvme_queue_t* nvme_setup_adminq(nvme_device_t* dev, int qsize,
                                void* sqbuf, u64 sqpa, void* cqbuf, u64 cqpa)
{
   if (nvme_ctlr_disable(dev)) return NULL;

   nvme_controller_cap_t cap;
   cap.val = dev->reg->cap.val;
   dev->dbstride = 1 << cap.dstrd; // in u32 size offset
   dev->maxqsize = cap.mqes + 1;
   dev->pageshift = PAGESHIFT;

   nvme_queue_t* adminq = &dev->adminq;
   adminq->dev = dev;
   adminq->id = 0;
   adminq->size = qsize;
   adminq->sq = (nvme_sq_entry_t*)sqbuf;
   adminq->cq = (nvme_cq_entry_t*)cqbuf;
   adminq->sq_doorbell = dev->reg->sq0tdbl;
   adminq->cq_doorbell = adminq->sq_doorbell + dev->dbstride;

   nvme_adminq_attr_t aqa;
   aqa.val = 0;
   aqa.asqs = aqa.acqs = qsize - 1;
   dev->reg->aqa.val = aqa.val;
   dev->reg->asq = sqpa;
   dev->reg->acq = cqpa;

   nvme_controller_config_t cc;
   cc.val = 0;
   cc.shn = 0;
   cc.ams = 0;
   cc.css = 0;
   cc.iosqes = 6;
   cc.iocqes = 4;
   cc.mps = dev->pageshift - 12;

   if (nvme_ctlr_enable(dev, cc)) return NULL;

   return adminq;
}

/**
 * Submit an entry at submission queue tail.
 * @param   q           queue
 */
static void nvme_submit_cmd(nvme_queue_t* q)
{
    if (++q->sq_tail == q->size) q->sq_tail = 0;
    *q->sq_doorbell = q->sq_tail;
}


/**
 * NVMe create I/O completion queue command.
 * Submit the command and wait for completion.
 * @param   ioq         io queue
 * @param   prp         PRP1 address
 * @param   ien         interrups enabled
 * @return  0 if ok, else -1.
 */
int nvme_acmd_create_cq(nvme_queue_t* ioq, u64 prp, int ien)
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
    cmd->ien = ien;
    cmd->iv = ioq->id;

    nvme_submit_cmd(adminq);
    return nvme_wait_completion(adminq, cid, 30);
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

    //DEBUG_FN("q=%d cid=%#x qs=%d", ioq->id, cid, ioq->size);
    nvme_submit_cmd(adminq);
    return nvme_wait_completion(adminq, cid, 30);
}

/**
 * Create an IO queue pair of completion and submission.
 * @param   dev         device context
 * @param   id          queue id
 * @param   qsize       queue size
 * @param   sqbuf       submission queue buffer
 * @param   sqpa        submission queue IO physical address
 * @param   cqbuf       completion queue buffer
 * @param   cqpa        admin completion IO physical address
 * @param   ien         interrupts enabled
 * @return  pointer to the created io queue or NULL if failure.
 */
int nvme_create_ioq(nvme_queue_t* ioq, nvme_device_t* dev, int id, int qsize,
                    void* sqbuf, u64 sqpa, void* cqbuf, u64 cqpa, int ien)
{
   ioq->dev = dev;
   ioq->id = id;
   ioq->size = qsize;
   ioq->sq = (nvme_sq_entry_t*)sqbuf;
   ioq->cq = (nvme_cq_entry_t*)cqbuf;
   ioq->sq_doorbell = dev->reg->sq0tdbl + (2 * id * dev->dbstride);
   ioq->cq_doorbell = ioq->sq_doorbell + dev->dbstride;

   if (nvme_acmd_create_cq(ioq, cqpa, ien) || nvme_acmd_create_sq(ioq, sqpa)) {
      return -1;
   }
   return 0;
}


/**
 * Check a completion queue and return the completed command id and status.
 * @param   q           queue
 * @param   stat        completion status reference
 * @return  the completed command id or -1 if there's no completion.
 */
int nvme_check_completion(nvme_queue_t* q, int* stat)
{
    nvme_cq_entry_t* cqe = &q->cq[q->cq_head];
    if (cqe->p == q->cq_phase) return -1;

    *stat = cqe->psf & 0xfe;
    if (++q->cq_head == q->size) {
        q->cq_head = 0;
        q->cq_phase = !q->cq_phase;
    }
    *q->cq_doorbell = q->cq_head;

    if (*stat == 0) {
       //DEBUG_FN("q=%d cid=%#x (C)", q->id, cqe->cid);
    } else {
       std::cout << "sth" << std::endl;
       //ERROR("q=%d cid=%#x stat=%#x (dnr=%d m=%d sct=%d sc=%#x) (C)",
       //q->id, cqe->cid, *stat, cqe->dnr, cqe->m, cqe->sct, cqe->sc);
    }
    return cqe->cid;
}

/**
 * Wait for a given command completion until timeout.
 * @param   q           queue
 * @param   cid         cid
 * @param   timeout in seconds
 * @return  completion status (0 if ok).
 */
int nvme_wait_completion(nvme_queue_t* q, int cid, int timeout)
{
    u64 endtsc = 0;

    do {
        int stat;
        int ret = nvme_check_completion(q, &stat);
        if (ret >= 0) {
            if (ret == cid && stat == 0) return 0;
            if (ret != cid) {
               std::cout << "cid wait " << cid << " " << ret << std::endl;
               stat = -1;
            } else {
               std::cout << "status " << stat << std::endl;
            }
            return stat;
        } else if (endtsc == 0) {
            endtsc = rdtsc() + timeout * rdtsc_second();
        }
    } while (rdtsc() < endtsc);

    std::cout << "timeout in nvme_wait_completion" << std::endl;
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

    nvme_submit_cmd(adminq);
    return nvme_wait_completion(adminq, cid, 30);
}

void print_controller(void* buf) {
   nvme_identify_ctlr_t* ctlr = (nvme_identify_ctlr_t*)buf;

   printf("Identify Controller\n");
   printf("===================\n");
   printf("vid     : %d\n", ctlr->vid);
   printf("ssvid   : %d\n", ctlr->ssvid);
   printf("sn      : %.20s\n", ctlr->sn);
   printf("mn      : %.40s\n", ctlr->mn);
   printf("fr      : %.8s\n", ctlr->fr);
   printf("rab     : %d\n", ctlr->rab);
   printf("ieee    : %02x%02x%02x\n", ctlr->ieee[0], ctlr->ieee[1], ctlr->ieee[2]);
   printf("mic     : %d\n", ctlr->mic);
   printf("mdts    : %d\n", ctlr->mdts);
   printf("oacs    : %d\n", ctlr->oacs);
   printf("acl     : %d\n", ctlr->acl);
   printf("aerl    : %d\n", ctlr->aerl);
   printf("frmw    : %d\n", ctlr->frmw);
   printf("lpa     : %d\n", ctlr->lpa);
   printf("elpe    : %d\n", ctlr->elpe);
   printf("npss    : %d\n", ctlr->npss);
   printf("avscc   : %d\n", ctlr->avscc);
   printf("sqes    : %d\n", ctlr->sqes);
   printf("cqes    : %d\n", ctlr->cqes);
   printf("nn      : %d\n", ctlr->nn);
   printf("oncs    : %d\n", ctlr->oncs);
   printf("fuses   : %d\n", ctlr->fuses);
   printf("fna     : %d\n", ctlr->fna);
   printf("vwc     : %d\n", ctlr->vwc);
   printf("awun    : %d\n", ctlr->awun);
   printf("awupf   : %d\n", ctlr->awupf);
   printf("nvscc   : %d\n", ctlr->nvscc);
}

void print_namespace(void* buf) {
   nvme_identify_ns_t* ns = (nvme_identify_ns_t*)buf;

   printf("Identify Namespace\n");
   printf("==================\n");
   printf("nsze    : %lu\n", ns->nsze);
   printf("ncap    : %lu\n", ns->ncap);
   printf("nuse    : %lu\n", ns->nuse);
   printf("nsfeat  : %d\n", ns->nsfeat);
   printf("nlbaf   : %d\n", ns->nlbaf);
   printf("flbas   : %d\n", ns->flbas);
   printf("mc      : %d\n", ns->mc);
   printf("dpc     : %d\n", ns->dpc);
   printf("dps     : %d\n", ns->dps);

   int i;
   for (i = 0; i <= ns->nlbaf; i++) {
      printf("lbaf.%-2d : ms=%-3d lbads=%-3d rp=%d  %s\n",
             i, ns->lbaf[i].ms, ns->lbaf[i].lbads, ns->lbaf[i].rp,
             (ns->flbas & 0xf) == i ? "(formatted)" : "");
   }
}
