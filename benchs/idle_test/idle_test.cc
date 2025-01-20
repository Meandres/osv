#include <iostream>
#include <chrono>
#include <thread>
#include <osv/cache.hh>
#include <drivers/nvme.hh>

using namespace std;

int main()
{
  CacheManager* cm = createMMIORegion(NULL, 1000*4096, 1000*4096, 64);
  Page p;
  memcpy(&p, cm->virtMem, 4096);
  PTE pte1 = walk(cm->virtMem); 
  memset(cm->virtMem, 1, 4096);
  PTE pte2 = walk(cm->virtMem); 
  atomic<u64>* pteRef = walkRef(cm->virtMem);
  PTE newPTE = PTE(pte2);
  newPTE.accessed = 1; newPTE.dirty = 1;
  pteRef->compare_exchange_strong(pte2.word, newPTE.word);
  memset(cm->virtMem, 1, 4096);
  PTE pte3 = walk(cm->virtMem); 
  printf("accessed: %u, dirty: %u\n", pte1.accessed, pte1.dirty);
  printf("accessed: %u, dirty: %u\n", pte2.accessed, pte2.dirty);
  printf("accessed: %u, dirty: %u\n", pte3.accessed, pte3.dirty);
  /*void *page0=(void*)cm->virtMem, *page1 = (void*)cm->virtMem+4096, *page2 = (void*)cm->virtMem+4096+4096;
  memset(page0, 1, 4096);
  memset(page1, 42, 4096);
  memset(page2, 1000, 4096);
  const unvme_ns_t* ns = unvme_openq(1, 10);
  unvme_iod_t iod1 = unvme_awrite(ns, 0, page0, 0, 8);
  unvme_iod_t iod2 = unvme_awrite(ns, 0, page1, 8, 8);
  unvme_iod_t iod3 = unvme_awrite(ns, 0, page2, 16, 8);
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  nvme_queue_t* qp = reinterpret_cast<unvme_desc_t*>(iod1)->q->nvmeq;
  cout << "sizeof cqe " << sizeof(nvme_cq_entry_t) << ", phase: " << qp->cq_phase << endl;
  for(int i=0; i<10; i++){
    nvme_cq_entry_t* cqe = &qp->cq[i];
    cout << cqe->p << ", " << cqe->rsvd3 << endl;
  }
  cout << "global cq_head: " << qp->cq_head << endl;
  int stat1, stat2, stat3;
  int ret2 = nvme_check_completion_ooo(reinterpret_cast<unvme_desc_t*>(iod2)->q->nvmeq, &stat2, NULL, 1);
  for(int i=0; i<10; i++){
    nvme_cq_entry_t* cqe = &qp->cq[i];
    cout << cqe->p << ", " << cqe->rsvd3 << endl;
  }
  cout << "global cq_head: " << qp->cq_head << endl;
  int ret1 = nvme_check_completion(reinterpret_cast<unvme_desc_t*>(iod1)->q->nvmeq, &stat1, NULL);
  int ret3 = nvme_check_completion(reinterpret_cast<unvme_desc_t*>(iod3)->q->nvmeq, &stat3, NULL);
  cout << "global cq_head: " << qp->cq_head << endl;
  cout << "ret1: " << ret1 << ", ret2: " << ret2 << ", ret3: " << ret3 << endl;*/
  return 0;
}
