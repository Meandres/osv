#include <osv/ucache.hh>
#include <osv/mmu.hh>
#include <osv/mempool.hh>
#include <osv/sched.hh>
#include <osv/vnode.h>
#include <osv/mount.h>

#include <vector>
#include <thread>
#include <bitset>
#include <atomic>
#include <cassert>
#include <functional>
#include <iostream>

#include <ext4_errno.h>

using namespace std;

namespace ucache {

ufs::ufs(): fs(NULL){
}

void ufs::add_device(const unvme_ns_t* ns){
		devices.push_back(ns);
}

ufile* ufs::open_ufile(const char* name){
    ufile* nuf = new ufile(name, this);
    struct vnode *vp = nuf->fp->f_dentry->d_vnode;
    if(fs == NULL){
        fs = (ext4_fs*)vp->v_mount->m_data;
    }
    assert_crash(ext4_fs_get_inode_ref(fs, vp->v_ino, nuf->inode) == EOK);
    return nuf;
}

ufile::ufile(const char* n, ufs* filesys){
    fs = filesys;
    current_seek_pos = 0;
    name = (char*)malloc(strlen(n)*sizeof(char));
    strcpy(name, n);
    int fd = open(name, O_RDONLY);
    fget(fd, &fp);
    assert_crash(fp != NULL);
    struct stat stats;
    fp->stat(&stats);
    size = stats.st_size;
}

void ufile::getLBAs(std::vector<u64>& blocks, u64 offset, u64 size){
    assert_crash(offset % ext4_block_size == 0); // make sure its 4KiB aligned
    u64 idx = 0, l_idx = offset/ext4_block_size;
    ext4_fs_get_inode_dblk_idx(inode, l_idx, &idx, true);
    blocks.push_back(idx);

    // coalesce blocks if they are contiguous ?
    if(size > ext4_block_size){
        for(u64 off = ext4_block_size; off < size; off += ext4_block_size){
            l_idx++;
            ext4_fs_get_inode_dblk_idx(inode, l_idx, &idx, true);
            blocks.push_back(idx);
        }
    }
}

void ufile::read(void* addr, u64 offset, u64 size){
    std::vector<u64> blocks;
    getLBAs(blocks, offset, size);
    assert_crash(!blocks.empty());
		int ret = unvme_read(fs->devices[0], sched::cpu::current()->id, addr, blocks[0], ext4_block_size/lb_size);
		if(blocks.size() > 1){
        for(size_t i = 1; i < blocks.size(); i++)
            ret += unvme_read(fs->devices[0], sched::cpu::current()->id, addr+i*ext4_block_size, blocks[i], ext4_block_size/lb_size);
    }
		assert_crash(ret == 0);
}

void ufile::write(void* addr, u64 offset, u64 size){
    std::vector<u64> blocks;
    getLBAs(blocks, offset, size);
    assert_crash(!blocks.empty());
		int ret = unvme_write(fs->devices[0], sched::cpu::current()->id, addr, blocks[0], ext4_block_size/lb_size);
		if(blocks.size() > 1){
        for(size_t i = 1; i < blocks.size(); i++)
            ret += unvme_write(fs->devices[0], sched::cpu::current()->id, addr+i*ext4_block_size, blocks[i], ext4_block_size/lb_size);
    }
		assert_crash(ret == 0);
}

aio_req_t* ufile::aread(void* addr, u64 offset, u64 size){
    std::vector<u64> blocks;
    getLBAs(blocks, offset, size);
    assert_crash(!blocks.empty());
    aio_req_t* reqs = new aio_req_t(blocks.size());
		reqs->nvme_iods[0] = unvme_aread(fs->devices[0], sched::cpu::current()->id, addr, blocks[0], ext4_block_size/lb_size);
		if(blocks.size() > 1){
        for(size_t i = 1; i < blocks.size(); i++)
            reqs->nvme_iods[i] = unvme_aread(fs->devices[0], sched::cpu::current()->id, addr+i*ext4_block_size, blocks[i], ext4_block_size/lb_size);
    }
    return reqs;
}

aio_req_t* ufile::awrite(void* addr, u64 offset, u64 size){
    std::vector<u64> blocks;
    getLBAs(blocks, offset, size);
    assert_crash(!blocks.empty());
    aio_req_t* reqs = new aio_req_t(blocks.size());
		reqs->nvme_iods[0] = unvme_awrite(fs->devices[0], sched::cpu::current()->id, addr, blocks[0], ext4_block_size/lb_size, ring);
		if(blocks.size() > 1){
        for(size_t i = 1; i < blocks.size(); i++)
            reqs->nvme_iods[i] = unvme_awrite(fs->devices[0], sched::cpu::current()->id, addr+i*ext4_block_size, blocks[i], ext4_block_size/lb_size, ring);
    }
    return reqs;
}

void ufile::poll(aio_req_t* reqs){
    assert_crash(unvme_apoll(reqs->nvme_iods[0], UNVME_SHORT_TIMEOUT) == 0);
    for(size_t i = 1; i<reqs->size; i++){
        assert_crash(unvme_apoll(reqs->nvme_iods[i], UNVME_SHORT_TIMEOUT) == 0);
    }
}

void ufile::commit_io(){ //std::vector<int> &devs){
		unvme_ring_sq_doorbell(fs->devices[0], sched::cpu::current()->id);
}

}
