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

#ifndef NVME_HH
#define NVME_HH

#include <osv/types.h>
#include <stdlib.h>
#include <pthread.h>

#include <stdio.h>
#include <ctype.h>

#include <stdint.h>
#include <sched.h>

#include "drivers/driver.hh"
#include "drivers/pci-device.hh"
#include <osv/interrupt.hh>
#include <osv/msi.hh>

/**
 * Get tsc per second using sleeping for 1/100th of a second.
 */
static inline uint64_t rdtsc_second()
{
    static uint64_t tsc_ps = 0;
    if (!tsc_ps) {
        uint64_t t0 = processor::rdtsc();
        usleep(1000);
        uint64_t t1 = processor::rdtsc();
        usleep(1000);
        uint64_t t2 = processor::rdtsc();
        t2 -= t1;
        t1 -= t0;
        if (t2 > t1) t2 = t1;
        tsc_ps = t2 * 1000;
    }
    return tsc_ps;
}

/// VFIO dma allocation structure
typedef struct _vfio_dma {
    void*                   buf;        ///< memory buffer
    size_t                  size;       ///< allocated size
    u64                   addr;       ///< I/O DMA address
} vfio_dma_t;

// Export functions
vfio_dma_t* vfio_dma_alloc(size_t size);
int vfio_dma_free(vfio_dma_t* dma);

#define INFO(fmt, arg...)     log_msg(NULL, fmt "\n", ##arg)
#define INFO_FN(fmt, arg...)  log_msg(NULL, "%s " fmt "\n", __func__, ##arg)
#define ERROR(fmt, arg...)    log_msg(stderr, "ERROR: %s " fmt "\n", __func__, ##arg)

#ifdef UNVME_DEBUG
    #define DEBUG             INFO
    #define DEBUG_FN          INFO_FN
    #define HEX_DUMP          hex_dump
#else
    #define DEBUG(arg...)
    #define DEBUG_FN(arg...)
    #define HEX_DUMP(arg...)
#endif

int log_open(const char* filename, const char* mode);
void log_close();
void log_msg(FILE* ftee, const char* fmt, ...);

/**
 * Hex dump a data block byte-wise.
 * @param   buf     buffer to read into
 * @param   len     size
 */
static inline void hex_dump(void* buf, int len)
{
    unsigned char* b = (unsigned char*)buf;
    int i, k = 0, e = 44, t = 44;
    char ss[3906];

    if (len > 1024) len = 1024;

    for (i = 0; i < len; i++) {
        if (!(i & 15)) {
            if (i > 0) {
                ss[k] = ' ';
                ss[t++] = '\n';
                k = t;
            }
            e = t = k + 44;
            k += sprintf(ss+k, "  %04x:", i);
        }
        if (!(i & 3)) ss[k++] = ' ';
        k += sprintf(ss+k, "%02x", b[i]);
        ss[t++] = isprint(b[i]) ? b[i] : '.';
    }
    ss[t] = 0;
    for (i = k; i < e; i++) ss[i] = ' ';
    INFO("%s", ss);
}

/**
 * Invoke calloc and terminated on failure.
 */
static inline void* zalloc(int size)
{
    void* mem = calloc(1, size);
    if (!mem) {
        ERROR("calloc");
        abort();
    }
    return mem;
}

#if __BYTE_ORDER__ != __ORDER_LITTLE_ENDIAN__
    #pragma error "only support little endian CPU architecture"
#endif


typedef struct vfio_iommu_type1_dma_map nvme_dma_t; ///< dma map structure

/// NVMe command op code
enum {
    NVME_CMD_FLUSH          = 0x0,      ///< flush
    NVME_CMD_WRITE          = 0x1,      ///< write
    NVME_CMD_READ           = 0x2,      ///< read
    NVME_CMD_WRITE_UNCOR    = 0x4,      ///< write uncorrectable
    NVME_CMD_COMPARE        = 0x5,      ///< compare
    NVME_CMD_DS_MGMT        = 0x9,      ///< dataset management
};

/// NVMe admin command op code
enum {
    NVME_ACMD_DELETE_SQ     = 0x0,      ///< delete io submission queue
    NVME_ACMD_CREATE_SQ     = 0x1,      ///< create io submission queue
    NVME_ACMD_GET_LOG_PAGE  = 0x2,      ///< get log page
    NVME_ACMD_DELETE_CQ     = 0x4,      ///< delete io completion queue
    NVME_ACMD_CREATE_CQ     = 0x5,      ///< create io completion queue
    NVME_ACMD_IDENTIFY      = 0x6,      ///< identify
    NVME_ACMD_ABORT         = 0x8,      ///< abort
    NVME_ACMD_SET_FEATURES  = 0x9,      ///< set features
    NVME_ACMD_GET_FEATURES  = 0xA,      ///< get features
    NVME_ACMD_ASYNC_EVENT   = 0xC,      ///< asynchronous event
    NVME_ACMD_FW_ACTIVATE   = 0x10,     ///< firmware activate
    NVME_ACMD_FW_DOWNLOAD   = 0x11,     ///< firmware image download
};

/// NVMe feature identifiers
enum {
    NVME_FEATURE_ARBITRATION = 0x1,     ///< arbitration
    NVME_FEATURE_POWER_MGMT = 0x2,      ///< power management
    NVME_FEATURE_LBA_RANGE = 0x3,       ///< LBA range type
    NVME_FEATURE_TEMP_THRESHOLD = 0x4,  ///< temperature threshold
    NVME_FEATURE_ERROR_RECOVERY = 0x5,  ///< error recovery
    NVME_FEATURE_WRITE_CACHE = 0x6,     ///< volatile write cache
    NVME_FEATURE_NUM_QUEUES = 0x7,      ///< number of queues
    NVME_FEATURE_INT_COALESCING = 0x8,  ///< interrupt coalescing
    NVME_FEATURE_INT_VECTOR = 0x9,      ///< interrupt vector config
    NVME_FEATURE_WRITE_ATOMICITY = 0xA, ///< write atomicity
    NVME_FEATURE_ASYNC_EVENT = 0xB,     ///< async event config
};

/// Version
typedef union _nvme_version {
    u32                 val;            ///< whole value
    struct {
        u8              rsvd;           ///< reserved
        u8              mnr;            ///< minor version number
        u16             mjr;            ///< major version number
    };
} nvme_version_t;

/// Admin queue attributes
typedef union _nvme_adminq_attr {
    u32                 val;            ///< whole value
    struct {
        u16             asqs;           ///< admin submission queue size
        u16             acqs;           ///< admin completion queue size
    };
} nvme_adminq_attr_t;

/// Controller capabilities
typedef union _nvme_controller_cap {
    u64                 val;            ///< whole value
    struct {
        u16             mqes;           ///< max queue entries supported
        u8              cqr     : 1;    ///< contiguous queues required
        u8              ams     : 2;    ///< arbitration mechanism supported
        u8              rsvd    : 5;    ///< reserved
        u8              to;             ///< timeout

        u32             dstrd   : 4;    ///< doorbell stride
        u32             nssrs   : 1;    ///< NVM subsystem reset supported
        u32             css     : 8;    ///< command set supported
        u32             rsvd2   : 3;    ///< reserved
        u32             mpsmin  : 4;    ///< memory page size minimum
        u32             mpsmax  : 4;    ///< memory page size maximum
        u32             rsvd3   : 8;    ///< reserved
    };
} nvme_controller_cap_t;

/// Controller configuration register
typedef union _nvme_controller_config {
    u32                 val;            ///< whole value
    struct {
        u32             en      : 1;    ///< enable
        u32             rsvd    : 3;    ///< reserved
        u32             css     : 3;    ///< I/O command set selected
        u32             mps     : 4;    ///< memory page size
        u32             ams     : 3;    ///< arbitration mechanism selected
        u32             shn     : 2;    ///< shutdown notification
        u32             iosqes  : 4;    ///< I/O submission queue entry size
        u32             iocqes  : 4;    ///< I/O completion queue entry size
        u32             rsvd2   : 8;    ///< reserved
    };
} nvme_controller_config_t;

/// Controller status register
typedef union _nvme_controller_status {
    u32                 val;            ///< whole value
    struct {
        u32             rdy     : 1;    ///< ready
        u32             cfs     : 1;    ///< controller fatal status
        u32             shst    : 2;    ///< shutdown status
        u32             rsvd    : 28;   ///< reserved
    };
} nvme_controller_status_t;

/// Controller memory buffer location register
typedef union _nvme_cmbloc {
    u32                 val;            ///< whole value
    struct {
        u32             bir     : 3;    ///< base indicator register
        u32             rsvd    : 9;    ///< reserved
        u32             ofst    : 20;   ///< offset (in cmbsz units)
    };
} nvme_cmbloc_t;

/// Controller memory buffer size register
typedef union _nvme_cmbsz {
    u32                 val;            ///< whole value
    struct {
        u32             sqs     : 1;    ///< submission queue support
        u32             cqs     : 1;    ///< completion queue support
        u32             lists   : 1;    ///< PRP SGL list support
        u32             rds     : 1;    ///< read data support
        u32             wds     : 1;    ///< write data support
        u32             rsvd    : 3;    ///< reserved
        u32             szu     : 4;    ///< size units (0=4K,1=64K,2=1M,3=16M,4=256M,5=4G,6=64G)
        u32             sz      : 20;   ///< size (in cmbsz units)
    };
} nvme_cmbsz_t;

/// Controller register (bar 0)
typedef struct _nvme_controller_reg {
    nvme_controller_cap_t   cap;        ///< controller capabilities
    nvme_version_t          vs;         ///< version
    u32                     intms;      ///< interrupt mask set
    u32                     intmc;      ///< interrupt mask clear
    nvme_controller_config_t cc;        ///< controller configuration
    u32                     rsvd;       ///< reserved
    nvme_controller_status_t csts;      ///< controller status
    u32                     nssr;       ///< NVM subsystem reset
    nvme_adminq_attr_t      aqa;        ///< admin queue attributes
    u64                     asq;        ///< admin submission queue base address
    u64                     acq;        ///< admin completion queue base address
    nvme_cmbloc_t           cmbloc;     ///< controller memory buffer location
    nvme_cmbsz_t            cmbsz;      ///< controller memory buffer size
    u32                     rcss[1008]; ///< reserved and command set specific
    u32                     sq0tdbl[1024]; ///< sq0 tail doorbell at 0x1000
} nvme_controller_reg_t;

/// Common command header (cdw 0-9)
typedef struct _nvme_command_common {
    u8                      opc;        ///< opcode
    u8                      fuse : 2;   ///< fuse
    u8                      rsvd : 6;   ///< reserved
    u16                     cid;        ///< command id
    u32                     nsid;       ///< namespace id
    u64                     cdw2_3;     ///< reserved (cdw 2-3)
    u64                     mptr;       ///< metadata pointer
    u64                     prp1;       ///< PRP entry 1
    u64                     prp2;       ///< PRP entry 2
} nvme_command_common_t;

/// NVMe command:  Read & Write
typedef struct _nvme_command_rw {
    nvme_command_common_t   common;     ///< common cdw 0
    u64                     slba;       ///< starting LBA (cdw 10)
    u16                     nlb;        ///< number of logical blocks
    u16                     rsvd12 : 10; ///< reserved (in cdw 12)
    u16                     prinfo : 4; ///< protection information field
    u16                     fua : 1;    ///< force unit access
    u16                     lr  : 1;    ///< limited retry
    u8                      dsm;        ///< dataset management
    u8                      rsvd13[3];  ///< reserved (in cdw 13)
    u32                     eilbrt;     ///< exp initial block reference tag
    u16                     elbat;      ///< exp logical block app tag
    u16                     elbatm;     ///< exp logical block app tag mask
} nvme_command_rw_t;

/// Admin and NVM Vendor Specific Command
typedef struct _nvme_command_vs {
    nvme_command_common_t   common;     ///< common cdw 0
    union {
        struct {
            u32             ndt;        ///< number of dwords data transfer
            u32             ndm;        ///< number of dwords metadata transfer
            u32             cdw12_15[4]; ///< vendor specific (cdw 12-15)
        };
        u32                 cdw10_15[6]; ///< vendor specific (cdw 10-15)
    };
} nvme_command_vs_t;

/// Admin command:  Delete I/O Submission & Completion Queue
typedef struct _nvme_acmd_delete_ioq {
    nvme_command_common_t   common;     ///< common cdw 0
    u16                     qid;        ///< queue id (cdw 10)
    u16                     rsvd10;     ///< reserved (in cdw 10)
    u32                     cdw11_15[5]; ///< reserved (cdw 11-15)
} nvme_acmd_delete_ioq_t;

/// Admin command:  Create I/O Submission Queue
typedef struct _nvme_acmd_create_sq {
    nvme_command_common_t   common;     ///< common cdw 0
    u16                     qid;        ///< queue id (cdw 10)
    u16                     qsize;      ///< queue size
    u16                     pc : 1;     ///< physically contiguous
    u16                     qprio : 2;  ///< interrupt enabled
    u16                     rsvd11 : 13; ///< reserved (in cdw 11)
    u16                     cqid;       ///< associated completion queue id
    u32                     cdw12_15[4]; ///< reserved (cdw 12-15)
} nvme_acmd_create_sq_t;

/// Admin command:  Get Log Page
typedef struct _nvme_acmd_get_log_page {
    nvme_command_common_t   common;     ///< common cdw 0
    u8                      lid;        ///< log page id (cdw 10)
    u8                      rsvd10a;    ///< reserved (in cdw 10)
    u16                     numd : 12;  ///< number of dwords
    u16                     rsvd10b : 4; ///< reserved (in cdw 10)
    u32                     rsvd11[5];  ///< reserved (cdw 11-15)
} nvme_acmd_get_log_page_t;

/// Admin command:  Create I/O Completion Queue
typedef struct _nvme_acmd_create_cq {
    nvme_command_common_t   common;     ///< common cdw 0
    u16                     qid;        ///< queue id (cdw 10)
    u16                     qsize;      ///< queue size
    u16                     pc : 1;     ///< physically contiguous
    u16                     ien : 1;    ///< interrupt enabled
    u16                     rsvd11 : 14; ///< reserved (in cdw 11)
    u16                     iv;         ///< interrupt vector
    u32                     cdw12_15[4]; ///< reserved (cdw 12-15)
} nvme_acmd_create_cq_t;

/// Admin command:  Identify
typedef struct _nvme_acmd_identify {
    nvme_command_common_t   common;     ///< common cdw 0
    u32                     cns;        ///< controller or namespace (cdw 10)
    u32                     cdw11_15[5]; ///< reserved (cdw 11-15)
} nvme_acmd_identify_t;

/// Admin command:  Abort
typedef struct _nvme_acmd_abort {
    nvme_command_common_t   common;     ///< common cdw 0
    u16                     sqid;       ///< submission queue id (cdw 10)
    u16                     cid;        ///< command id
    u32                     cdw11_15[5]; ///< reserved (cdw 11-15)
} nvme_acmd_abort_t;

/// Admin data:  Identify Controller Data
typedef struct _nvme_identify_ctlr {
    u16                     vid;        ///< PCI vendor id
    u16                     ssvid;      ///< PCI subsystem vendor id
    char                    sn[20];     ///< serial number
    char                    mn[40];     ///< model number
    char                    fr[8];      ///< firmware revision
    u8                      rab;        ///< recommended arbitration burst
    u8                      ieee[3];    ///< IEEE OUI identifier
    u8                      mic;        ///< multi-interface capabilities
    u8                      mdts;       ///< max data transfer size
    u8                      rsvd78[178]; ///< reserved (78-255)
    u16                     oacs;       ///< optional admin command support
    u8                      acl;        ///< abort command limit
    u8                      aerl;       ///< async event request limit
    u8                      frmw;       ///< firmware updates
    u8                      lpa;        ///< log page attributes
    u8                      elpe;       ///< error log page entries
    u8                      npss;       ///< number of power states support
    u8                      avscc;      ///< admin vendor specific config
    u8                      rsvd265[247]; ///< reserved (265-511)
    u8                      sqes;       ///< submission queue entry size
    u8                      cqes;       ///< completion queue entry size
    u8                      rsvd514[2]; ///< reserved (514-515)
    u32                     nn;         ///< number of namespaces
    u16                     oncs;       ///< optional NVM command support
    u16                     fuses;      ///< fused operation support
    u8                      fna;        ///< format NVM attributes
    u8                      vwc;        ///< volatile write cache
    u16                     awun;       ///< atomic write unit normal
    u16                     awupf;      ///< atomic write unit power fail
    u8                      nvscc;      ///< NVM vendor specific config
    u8                      rsvd531[173]; ///< reserved (531-703)
    u8                      rsvd704[1344]; ///< reserved (704-2047)
    u8                      psd[1024];  ///< power state 0-31 descriptors
    u8                      vs[1024];   ///< vendor specific
} nvme_identify_ctlr_t;

/// Admin data:  Identify Namespace - LBA Format Data
typedef struct _nvme_lba_format {
    u16                     ms;         ///< metadata size
    u8                      lbads;      ///< LBA data size
    u8                      rp : 2;     ///< relative performance
    u8                      rsvd : 6;   ///< reserved
} nvme_lba_format_t;

/// Admin data:  Identify Namespace Data
typedef struct _nvme_identify_ns {
    u64                     nsze;       ///< namespace size
    u64                     ncap;       ///< namespace capacity
    u64                     nuse;       ///< namespace utilization
    u8                      nsfeat;     ///< namespace features
    u8                      nlbaf;      ///< number of LBA formats
    u8                      flbas;      ///< formatted LBA size
    u8                      mc;         ///< metadata capabilities
    u8                      dpc;        ///< data protection capabilities
    u8                      dps;        ///< data protection settings
    u8                      rsvd30[98]; ///< reserved (30-127)
    nvme_lba_format_t       lbaf[16];   ///< lba format support
    u8                      rsvd192[192]; ///< reserved (383-192)
    u8                      vs[3712];   ///< vendor specific
} nvme_identify_ns_t;

/// Admin data:  Get Log Page - Error Information
typedef struct _nvme_log_page_error {
    u64                     count;      ///< error count
    u16                     sqid;       ///< submission queue id
    u16                     cid;        ///< command id
    u16                     sf;         ///< status field
    u8                      byte;       ///< parameter byte error location
    u8                      bit: 3;     ///< parameter bit error location
    u8                      rsvd : 5;   ///< reserved
    u64                     lba;        ///< logical block address
    u32                     ns;         ///< name space
    u8                      vspec;      ///< vendor specific infomation
    u8                      rsvd29[35]; ///< reserved (29-63)
} nvme_log_page_error_t;

/// Admin data:  Get Log Page - SMART / Health Information
typedef struct _nvme_log_page_health {
    u8                      warn;       ///< critical warning
    u16                     temp;       ///< temperature
    u8                      avspare;     ///< available spare
    u8                      avsparethresh; ///< available spare threshold
    u8                      used;       ///< percentage used
    u8                      rsvd6[26];  ///< reserved (6-31)
    u64                     dur[2];     ///< data units read
    u64                     duw[2];     ///< data units written
    u64                     hrc[2];     ///< number of host read commands
    u64                     hwc[2];     ///< number of host write commands
    u64                     cbt[2];     ///< controller busy time
    u64                     pcycles[2]; ///< number of power cycles
    u64                     phours[2]; ///< power on hours
    u64                     unsafeshut[2]; ///< unsafe shutdowns
    u64                     merrors[2]; ///< media errors
    u64                     errlogs[2]; ///< number of error log entries
    u64                     rsvd192[320]; ///< reserved (192-511)
} nvme_log_page_health_t;

/// Admin data:  Get Log Page - Firmware Slot Information
typedef struct _nvme_log_page_fw {
    u8                      afi;        ///< active firmware info
    u8                      rsvd1[7];   ///< reserved (1-7)
    u64                     fr[7];      ///< firmware revision for slot 1-7
    u8                      rsvd64[448]; ///< reserved (64-511)
} nvme_log_page_fw_t;

/// Admin feature:  Arbitration
typedef struct _nvme_feature_arbitration {
    u8                      ab: 3;      ///< arbitration burst
    u8                      rsvd: 5;    ///< reserved
    u8                      lpw;        ///< low priority weight
    u8                      mpw;        ///< medium priority weight
    u8                      hpw;        ///< high priority weight
} nvme_feature_arbitration_t;

/// Admin feature:  Power Management
typedef struct _nvme_feature_power_mgmt {
    u32                     ps: 5;      ///< power state
    u32                     rsvd: 27;   ///< reserved
} nvme_feature_power_mgmt_t;

/// Admin feature:  LBA Range Type Data
typedef struct _nvme_feature_lba_data {
    struct {
        u8                  type;       ///< type
        u8                  attributes; ///< attributes
        u8                  rsvd[14];   ///< reserved
        u64                 slba;       ///< starting LBA
        u64                 nlb;        ///< number of logical blocks
        u8                  guid[16];   ///< unique id
        u8                  rsvd48[16]; ///< reserved
    } entry[64];                        ///< LBA data entry
} nvme_feature_lba_data_t;

/// Admin feature:  LBA Range Type
typedef struct _nvme_feature_lba_range {
    u32                     num: 6;     ///< number of LBA ranges
    u32                     rsvd: 26;   ///< reserved
} nvme_feature_lba_range_t;

/// Admin feature:  Temperature Threshold
typedef struct _nvme_feature_temp_threshold {
    u16                     tmpth;      ///< temperature threshold
    u16                     rsvd;       ///< reserved
} nvme_feature_temp_threshold_t;

/// Admin feature:  Error Recovery
typedef struct _nvme_feature_error_recovery {
    u16                     tler;       ///< time limited error recovery
    u16                     rsvd;       ///< reserved
} nvme_feature_error_recovery_t;

/// Admin feature:  Volatile Write Cache
typedef struct _nvme_feature_write_cache {
    u32                     wce: 1;     ///< volatile write cache
    u32                     rsvd: 31;   ///< reserved
} nvme_feature_write_cache_t;

/// Admin feature:  Number of Queues
typedef struct _nvme_feature_num_queues {
    u16                     nsq;        ///< numer of submission queues
    u16                     ncq;        ///< numer of completion queues
} nvme_feature_num_queues_t;

/// Admin feature:  Interrupt Coalescing
typedef struct _nvme_feature_int_coalescing {
    u8                      thr;        ///< aggregation threshold
    u8                      time;       ///< aggregation time
    u16                     rsvd;       ///< reserved
} nvme_feature_int_coalescing_t;

/// Admin feature:  Interrupt Vector Configuration
typedef struct _nvme_feature_int_vector {
    u16                     iv;         ///< interrupt vector
    u16                     cd: 1;      ///< coalescing disable
    u16                     rsvd: 15;   ///< reserved
} nvme_feature_int_vector_t;

/// Admin feature:  Write Atomicity
typedef struct _nvme_feature_write_atomicity {
    u32                     dn: 1;      ///< disable normal
    u32                     rsvd: 31;   ///< reserved
} nvme_feature_write_atomicity_t;

/// Admin feature:  Async Event Configuration
typedef struct _nvme_feature_async_event {
    u8                      smart;      ///< SMART / health critical warnings
    u8                      rsvd[3];    ///< reserved
} nvme_feature_async_event_t;

/// Admin command:  Get Feature
typedef struct _nvme_acmd_get_features {
    nvme_command_common_t   common;     ///< common cdw 0
    u8                      fid;        ///< feature id (cdw 10:0-7)
    u8                      rsvd10[3];  ///< reserved (cdw 10:8-31)
} nvme_acmd_get_features_t;

/// Admin command:  Set Feature
typedef struct _nvme_acmd_set_features {
    nvme_command_common_t   common;     ///< common cdw 0
    u8                      fid;        ///< feature id (cdw 10:0-7)
    u8                      rsvd10[3];  ///< reserved (cdw 10:8-31)
    u32                     val;        ///< cdw 11
} nvme_acmd_set_features_t;

/// Submission queue entry
typedef union _nvme_sq_entry {
    nvme_command_rw_t       rw;         ///< read/write command
    nvme_command_vs_t       vs;         ///< admin and vendor specific command

    nvme_acmd_abort_t       abort;      ///< admin abort command
    nvme_acmd_create_cq_t   create_cq;  ///< admin create IO completion queue
    nvme_acmd_create_sq_t   create_sq;  ///< admin create IO submission queue
    nvme_acmd_delete_ioq_t  delete_ioq; ///< admin delete IO queue
    nvme_acmd_identify_t    identify;   ///< admin identify command
    nvme_acmd_get_log_page_t get_log_page; ///< get log page command
    nvme_acmd_get_features_t get_features; ///< get feature
    nvme_acmd_set_features_t set_features; ///< set feature
} nvme_sq_entry_t;

/// Completion queue entry
typedef struct _nvme_cq_entry {
    u32                     cs;         ///< command specific
    u32                     rsvd;       ///< reserved
    u16                     sqhd;       ///< submission queue head
    u16                     sqid;       ///< submission queue id
    u16                     cid;        ///< command id
    union {
        u16                 psf;        ///< phase bit and status field
        struct {
            u16             p : 1;      ///< phase tag id
            u16             sc : 8;     ///< status code
            u16             sct : 3;    ///< status code type
            u16             rsvd3 : 2;  ///< reserved
            u16             m : 1;      ///< more
            u16             dnr : 1;    ///< do not retry
        };
    };
} nvme_cq_entry_t;

struct _nvme_device;

/// Queue context (a submission-completion queue pair context)
typedef struct _nvme_queue {
    struct _nvme_device*    dev;        ///< device reference
    int                     id;         ///< queue id
    int                     size;       ///< queue size
    nvme_sq_entry_t*        sq;         ///< submission queue entries
    nvme_cq_entry_t*        cq;         ///< completion queue entries
    u32*                    sq_doorbell; ///< submission queue doorbell
    //std::atomic<u32>*       cq_doorbell; ///< completion queue doorbell
    u32*                    cq_doorbell; ///< completion queue doorbell
    int                     sq_head;    ///< submission queue head
    int                     sq_tail;    ///< submission queue tail
    //std::atomic<int>        cq_head;    ///< completion queue head
    int                     cq_head;    ///< completion queue head
    //std::atomic<u16>        cq_phase;   ///< completion queue phase bit
    u16                     cq_phase;   ///< completion queue phase bit
    u16                     ext;        ///< externally allocated flag
    u32                     last_cq_doorbelled;
} nvme_queue_t;

/// Device context
typedef struct _nvme_device {
    nvme_controller_reg_t*  reg;        ///< register address map
    nvme_queue_t            adminq;     ///< admin queue reference
    nvme_cmbloc_t           cmbloc;     ///< CMB location
    nvme_cmbsz_t            cmbsz;      ///< CMB size
    u64                     rdtsec;     ///< rdtsc per second
    u32                     maxqcount;  ///< max queue count
    u32                     maxqsize;   ///< max queue size
    u16                     dbstride;   ///< doorbell stride (in word size)
    u16                     timeout;    ///< in 500 ms units
    u16                     pageshift;  ///< minimum pagesize shift
    u16                     mpsmin;     ///< MPSMIN
    u16                     mpsmax;     ///< MPSMAX
    u16                     ext;        ///< externally allocated flag
} nvme_device_t;


// Export functions
nvme_device_t* nvme_create(nvme_device_t* dev);
void nvme_delete(nvme_device_t* dev);

nvme_queue_t* nvme_adminq_setup(nvme_device_t* dev, int qsize, void* sqbuf, u64 sqpa, void* cqbuf, u64 cqpa);
nvme_queue_t* nvme_ioq_create(nvme_device_t* dev, nvme_queue_t* ioq, int id, int qsize, void* sqbuf, u64 sqpa, void* cqbuf, u64 cqpa);
int nvme_ioq_delete(nvme_queue_t* ioq);

int nvme_acmd_identify(nvme_device_t* dev, int nsid, u64 prp1, u64 prp2);
int nvme_acmd_get_log_page(nvme_device_t* dev, int nsid, int lid, int numd, u64 prp1, u64 prp2);
int nvme_acmd_get_features(nvme_device_t* dev, int nsid, int fid, u64 prp1, u64 prp2, u32* res);
int nvme_acmd_set_features(nvme_device_t* dev, int nsid, int fid, u64 prp1, u64 prp2, u32* res);
int nvme_acmd_create_cq(nvme_queue_t* ioq, u64 prp);
int nvme_acmd_create_sq(nvme_queue_t* ioq, u64 prp);
int nvme_acmd_delete_cq(nvme_queue_t* ioq);
int nvme_acmd_delete_sq(nvme_queue_t* ioq);

int nvme_cmd_vs(nvme_queue_t* q, int opc, u16 cid, int nsid, u64 prp1, u64 prp2, u32 cdw10_15[6]);
int nvme_cmd_rw(nvme_queue_t* ioq, int opc, u16 cid, int nsid, u64 slba, int nlb, u64 prp1, u64 prp2, bool ring=true);
int nvme_cmd_read(nvme_queue_t* ioq, u16 cid, int nsid, u64 slba, int nlb, u64 prp1, u64 prp2);
int nvme_cmd_write(nvme_queue_t* ioq, u16 cid, int nsid, u64 slba, int nlb, u64 prp1, u64 prp2);

int nvme_check_completion(nvme_queue_t* q, int* stat, u32* cqe_cs);
int nvme_check_completion_ooo(nvme_queue_t* q, int* stat, u32* cqe_cs, int pos);
int nvme_wait_completion(nvme_queue_t* q, int cid, int timeout);

//extern std::vector<u32> last_cq_doorbelled;

/// Lock write bit
#define UNVME_LOCKWBIT      0x80000000

/// Simple read write lock
typedef unsigned unvme_lock_t;

/**
 * Increment read lock and wait if pending write.
 * @param   lock    lock variable
 */
static inline void unvme_lockr(unvme_lock_t* lock)
{
    for (;;) {
        if (!(__sync_fetch_and_add(lock, 1) & UNVME_LOCKWBIT)) return;
        __sync_fetch_and_sub(lock, 1);
        sched_yield();
    }
}

/**
 * Decrement read lock.
 * @param   lock    lock variable
 */
static inline void unvme_unlockr(unvme_lock_t* lock)
{
    __sync_fetch_and_sub(lock, 1);
}

/**
 * Acquire write lock and wait for all pending read/write.
 * @param   lock    lock variable
 */
static inline void unvme_lockw(unvme_lock_t* lock)
{
    for (;;) {
        int val = __sync_fetch_and_or(lock, UNVME_LOCKWBIT);
        if (val == 0) return;
        sched_yield();

        // if not pending write then just wait for all reads to clear
        if (!(val & UNVME_LOCKWBIT)) {
            while (*lock != UNVME_LOCKWBIT) sched_yield();
            return;
        }
    }
}

/**
 * Clear the write lock.
 * @param   lock    lock variable
 */
static inline void unvme_unlockw(unvme_lock_t* lock)
{
    __sync_fetch_and_and(lock, ~UNVME_LOCKWBIT);
}

/// Doubly linked list add node
#define LIST_ADD(head, node)                                    \
            if ((head) != NULL) {                               \
                (node)->next = (head);                          \
                (node)->prev = (head)->prev;                    \
                (head)->prev->next = (node);                    \
                (head)->prev = (node);                          \
            } else {                                            \
                (node)->next = (node)->prev = (node);           \
                (head) = (node);                                \
            }

/// Doubly linked list remove node
#define LIST_DEL(head, node)                                    \
            if ((node->next) != (node)) {                       \
                (node)->next->prev = (node)->prev;              \
                (node)->prev->next = (node)->next;              \
                if ((head) == (node)) (head) = (node)->next;    \
            } else {                                            \
                (head) = NULL;                                  \
            }

/// Log and print an unrecoverable error message and exit
#define FATAL(fmt, arg...)  do { ERROR(fmt, ##arg); abort(); } while (0)

/// Namespace attributes structure
typedef struct _unvme_ns {
    u16                 id;         ///< namespace id
    u16                 vid;        ///< vendor id
    char                mn[40];     ///< model number
    char                sn[20];     ///< serial number
    char                fr[8];      ///< firmware revision
    u64                 blockcount; ///< total number of available blocks
    u64                 pagecount;  ///< total number of available pages
    u16                 blocksize;  ///< logical block size
    u16                 pagesize;   ///< page size
    u16                 blockshift; ///< block size shift value
    u16                 pageshift;  ///< page size shift value
    u16                 bpshift;    ///< block to page shift
    u16                 nbpp;       ///< number of blocks per page
    u16                 maxbpio;    ///< max number of blocks per I/O
    u16                 maxppio;    ///< max number of pages per I/O
    u16                 maxiopq;    ///< max number of I/O submissions per queue
    u16                 nscount;    ///< number of namespaces available
    u32                 qcount;     ///< number of I/O queues
    u32                 maxqcount;  ///< max number of queues supported
    u32                 qsize;      ///< I/O queue size
    u32                 maxqsize;   ///< max queue size supported
    void*               ses;        ///< associated session
} unvme_ns_t;

/// I/O descriptor (not to be copied and is cleared upon apoll completion)
typedef struct _unvme_iod {
    void*               buf;        ///< data buffer (as submitted)
    u64                 slba;       ///< starting lba (as submitted)
    u32                 nlb;        ///< number of blocks (as submitted)
    u32                 qid;        ///< queue id (as submitted)
    u32                 opc;        ///< op code
    u32                 id;         ///< descriptor id
} *unvme_iod_t;

/// IO memory allocation tracking info
typedef struct _unvme_iomem {
    vfio_dma_t**            map;        ///< dynamic array of allocated memory
    int                     size;       ///< array size
    int                     count;      ///< array count
    unvme_lock_t            lock;       ///< map access lock
} unvme_iomem_t;

/// IO full descriptor
typedef struct _unvme_desc {
    void*                   buf;        ///< buffer
    u64                     slba;       ///< starting lba
    u32                     nlb;        ///< number of blocks
    u32                     qid;        ///< queue id
    u32                     opc;        ///< op code
    u32                     id;         ///< descriptor id
    void*                   sentinel;   ///< sentinel check
    struct _unvme_queue*    q;          ///< queue context owner
    struct _unvme_desc*     prev;       ///< previous descriptor node
    struct _unvme_desc*     next;       ///< next descriptor node
    int                     error;      ///< error status
    int                     cidcount;   ///< number of pending cids
    u64                     cidmask[];  ///< cid pending bit mask
} unvme_desc_t;

/// IO queue entry
typedef struct _unvme_queue {
    nvme_queue_t*           nvmeq;      ///< NVMe associated queue
    vfio_dma_t*             sqdma;      ///< submission queue mem
    vfio_dma_t*             cqdma;      ///< completion queue mem
    vfio_dma_t*             prplist;    ///< PRP list
    u32                     size;       ///< queue depth
    u16                     cid;        ///< next cid to check and use
    int                     cidcount;   ///< number of pending cids
    int                     desccount;  ///< number of pending descriptors
    int                     masksize;   ///< bit mask size to allocate
    u64*                    cidmask;    ///< cid pending bit mask
    unvme_desc_t*           desclist;   ///< used descriptor list
    unvme_desc_t*           descfree;   ///< free descriptor list
    unvme_desc_t*           descpend;   ///< pending descriptor list
} unvme_queue_t;

/// Device context
typedef struct _unvme_device {
    nvme_device_t           nvmedev;    ///< NVMe device
    unvme_queue_t           adminq;     ///< adminq queue
    int                     refcount;   ///< reference count
    unvme_iomem_t           iomem;      ///< IO memory tracker
    unvme_ns_t              ns;         ///< controller namespace (id=0)
    unvme_queue_t*          ioqs;       ///< pointer to IO queues
} unvme_device_t;

/// Session context
typedef struct _unvme_session {
    struct _unvme_session*  prev;       ///< previous session node
    struct _unvme_session*  next;       ///< next session node
    unvme_device_t*         dev;        ///< device context
    unvme_ns_t              ns;         ///< namespace
} unvme_session_t;

unvme_ns_t* unvme_do_open(int nsid, int qcount, int qsize);
int unvme_do_close(const unvme_ns_t* ns);
void* unvme_do_alloc(const unvme_ns_t* ns, u64 size);
int unvme_do_free(const unvme_ns_t* ses, void* buf);
int unvme_do_poll(unvme_desc_t* desc, int sec, u32* cqe_cs);
unvme_desc_t* unvme_do_cmd(const unvme_ns_t* ns, int qid, int opc, int nsid, void* buf, u64 bufsz, u32 cdw10_15[6]);
unvme_desc_t* unvme_do_rw(const unvme_ns_t* ns, int qid, int opc, void* buff, u64 slba, u32 nlb);
int unvme_check_completion(const unvme_ns_t* ns, int qid, int timeout=0);
void unvme_print_stats();


#define UNVME_TIMEOUT   60          ///< default timeout in seconds
#define UNVME_SHORT_TIMEOUT 5
#define UNVME_QSIZE     256         ///< default I/O queue size


// Export functions
const unvme_ns_t* unvme_open(void);
const unvme_ns_t* unvme_openq(int qcount, int qsize);
int unvme_close(const unvme_ns_t* ns);

void* unvme_alloc(const unvme_ns_t* ns, u64 size);
int unvme_free(const unvme_ns_t* ns, void* buf);

int unvme_write(const unvme_ns_t* ns, int qid, const void* buf, u64 slba, u32 nlb);
int unvme_read(const unvme_ns_t* ns, int qid, void* buf, u64 slba, u32 nlb);
int unvme_cmd(const unvme_ns_t* ns, int qid, int opc, int nsid, void* buf, u64 bufsz, u32 cdw10_15[6], u32* cqe_cs);

void unvme_ring_sq_doorbell(const unvme_ns_t* ns, int qid);
unvme_iod_t unvme_awrite(const unvme_ns_t* ns, int qid, const void* buf, u64 slba, u32 nlb, bool ring=true);
unvme_iod_t unvme_aread(const unvme_ns_t* ns, int qid, void* buf, u64 slba, u32 nlb, bool ring=true);
unvme_iod_t unvme_acmd(const unvme_ns_t* ns, int qid, int opc, int nsid, void* buf, u64 bufsz, u32 cdw10_15[6]);

int unvme_apoll(unvme_iod_t iod, int timeout);
int unvme_apoll_cs(unvme_iod_t iod, int timeout, u32* cqe_cs);

extern nvme_controller_reg_t* globalReg;

class nvme : public hw_driver {
public:
    explicit nvme(pci::device& dev);
    virtual ~nvme() {};

    virtual std::string get_name() const { return "nvme"; }

    virtual void dump_config(void);
    int transmit(struct mbuf* m_head);

    static hw_driver* probe(hw_device* dev);

    const unvme_ns_t* ns;
private:
    void parse_pci_config();
    void stop();
    void enable_device();
    void do_version_handshake();
    void attach_queues_shared(struct ifnet* ifn, pci::bar *bar0);
    void fill_driver_shared();
    void allocate_interrupts();

    virtual void isr() {};

    void write_cmd(u32 cmd);
    u32 read_cmd(u32 cmd);

    void get_mac_address(u_int8_t *macaddr);
    void enable_interrupts();
    void disable_interrupts();

    //maintains the nvme instance number for multiple adapters
    static int _instance;
    int _id;
    struct ifnet* _ifn;
    static int __disk_idx;

    pci::device& _dev;

    //Shared memory
    pci::bar *_bar0 = nullptr;
};

#endif
