#include <atomic>
#include <algorithm>
#include <cassert>
#include <csignal>
#include <exception>
#include <fcntl.h>
#include <functional>
#include <iostream>
#include <mutex>
#include <numeric>
#include <set>
#include <thread>
#include <vector>
#include <span>

#include <libaio.h>
#include <sys/mman.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#include <immintrin.h>

#include <osv/cache.hh>
#include <drivers/rdtsc.h>
#include <cstring>

#include "BTree.hpp"

#include "load_injector.hpp"
#include "tpcc/TPCCWorkload.hpp"

using namespace std;

typedef u64 KeyType;
CacheManager* cache=NULL;

u64 envOr(const char* env, u64 value) {
   if (getenv(env))
      return atof(getenv(env));
   return value;
}

template <class Record>
struct vmcacheAdapter
{
   BTree tree;

   public:

   vmcacheAdapter(): tree(envOr("VIRTGB", 4ull), envOr("PHYSGB", 1ull), envOr("THREADS", 1)) {}
   void scan(const typename Record::Key& key, const std::function<bool(const typename Record::Key&, const Record&)>& found_record_cb, std::function<void()> reset_if_scan_failed_cb) {
      u8 k[Record::maxFoldLength()];
      u16 l = Record::foldKey(k, key);
      u8 kk[Record::maxFoldLength()];
      tree.scanAsc({k, l}, [&](BTreeNode& node, unsigned slot) {
         memcpy(kk, node.getPrefix(), node.prefixLen);
         memcpy(kk+node.prefixLen, node.getKey(slot), node.slot[slot].keyLen);
         typename Record::Key typedKey;
         Record::unfoldKey(kk, typedKey);
         return found_record_cb(typedKey, *reinterpret_cast<const Record*>(node.getPayload(slot).data()));
      });
   }
   // -------------------------------------------------------------------------------------
   void scanDesc(const typename Record::Key& key, const std::function<bool(const typename Record::Key&, const Record&)>& found_record_cb, std::function<void()> reset_if_scan_failed_cb) {
      u8 k[Record::maxFoldLength()];
      u16 l = Record::foldKey(k, key);
      u8 kk[Record::maxFoldLength()];
      bool first = true;
      tree.scanDesc({k, l}, [&](BTreeNode& node, unsigned slot, bool exactMatch) {
         if (first) { // XXX: hack
            first = false;
            if (!exactMatch)
               return true;
         }
         memcpy(kk, node.getPrefix(), node.prefixLen);
         memcpy(kk+node.prefixLen, node.getKey(slot), node.slot[slot].keyLen);
         typename Record::Key typedKey;
         Record::unfoldKey(kk, typedKey);
         return found_record_cb(typedKey, *reinterpret_cast<const Record*>(node.getPayload(slot).data()));
      });
   }
   // -------------------------------------------------------------------------------------
   void insert(const typename Record::Key& key, const Record& record) {
      u8 k[Record::maxFoldLength()];
      u16 l = Record::foldKey(k, key);
      tree.insert({k, l}, {(u8*)(&record), sizeof(Record)});
   }
   // -------------------------------------------------------------------------------------
   template<class Fn>
   void lookup1(const typename Record::Key& key, Fn fn) {
      u8 k[Record::maxFoldLength()];
      u16 l = Record::foldKey(k, key);
      bool succ = tree.lookup({k, l}, [&](span<u8> payload) {
         fn(*reinterpret_cast<const Record*>(payload.data()));
      });
      assert(succ);
   }
   // -------------------------------------------------------------------------------------
   template<class Fn>
   void update1(const typename Record::Key& key, Fn fn) {
      u8 k[Record::maxFoldLength()];
      u16 l = Record::foldKey(k, key);
      tree.updateInPlace({k, l}, [&](span<u8> payload) {
         fn(*reinterpret_cast<Record*>(payload.data()));
      });
   }
   // -------------------------------------------------------------------------------------
   // Returns false if the record was not found
   bool erase(const typename Record::Key& key) {
      u8 k[Record::maxFoldLength()];
      u16 l = Record::foldKey(k, key);
      return tree.remove({k, l});
   }
   // -------------------------------------------------------------------------------------
   template <class Field>
   Field lookupField(const typename Record::Key& key, Field Record::*f) {
      Field value;
      lookup1(key, [&](const Record& r) { value = r.*f; });
      return value;
   }

   u64 count() {
      u64 cnt = 0;
      tree.scanAsc({(u8*)nullptr, 0}, [&](BTreeNode& node, unsigned slot) { cnt++; return true; });
      return cnt;
   }

   u64 countw(Integer w_id) {
      u8 k[sizeof(Integer)];
      fold(k, w_id);
      u64 cnt = 0;
      u8 kk[Record::maxFoldLength()];
      tree.scanAsc({k, sizeof(Integer)}, [&](BTreeNode& node, unsigned slot) {
         memcpy(kk, node.getPrefix(), node.prefixLen);
         memcpy(kk+node.prefixLen, node.getKey(slot), node.slot[slot].keyLen);
         if (memcmp(k, kk, sizeof(Integer))!=0)
            return false;
         cnt++;
         return true;
      });
      return cnt;
   }
};

int stick_this_thread_to_core(int core_id) {
   cpu_set_t cpuset;
   CPU_ZERO(&cpuset);
   CPU_SET(core_id, &cpuset);

   pthread_t current_thread = pthread_self();
   return pthread_setaffinity_np(current_thread, sizeof(cpu_set_t), &cpuset);
}

template<class Fn>
void parallel_for(uint64_t begin, uint64_t end, uint64_t nthreads, Fn fn) {
    std::vector<std::thread> threads;
    uint64_t n = end-begin;
    if (n<nthreads)
        nthreads = n;
    uint64_t perThread = n/nthreads;
    for (unsigned i=0; i<nthreads; i++) {
        threads.emplace_back([&,i]() {
            stick_this_thread_to_core(i);
            uint64_t b = (perThread*i) + begin;
            uint64_t e = (i==(nthreads-1)) ? end : ((b+perThread) + begin);
            fn(i, b, e);
        });
    }
   for (auto& t : threads)
      t.join();
}

int main(int argc, char** argv) {
   unsigned nthreads = envOr("THREADS", 1);
   u64 n = envOr("DATASIZE", 10);
   u64 runForSec = envOr("RUNFOR", 30);
   bool isRndread = envOr("RNDREAD", 0);

   u64 statDiff = 1e8;
   atomic<u64> txProgress(0);
   atomic<bool> keepRunning(true);
   auto systemName = "osv";

   auto statFn = [&]() {
      //cout << "ts,tx,rmb,wmb,pageFaults,system,threads,datasize,workload,batch" << endl;
      printf("ts,tx,rmb,wmb,system,threads,datasize,workload,batch\n");
      u64 cnt = 0;
      for (uint64_t i=0; i<runForSec; i++) {
         sleep(1);
         float rmb = (cache->readCount.exchange(0)*pageSize)/(1024.0*1024);
         float wmb = (cache->writeCount.exchange(0)*pageSize)/(1024.0*1024);
         u64 prog = txProgress.exchange(0);
         cout << cnt++ << "," << prog << "," << rmb << "," << wmb <<  "," << systemName << "," << nthreads << "," << n << "," << (isRndread?"rndread":"tpcc") << "," << cache->batch << endl;
         //printf("%lu,%lu,%lF%lF,%s,%u,%lu,%s,%lu\n", cnt++, prog, rmb, wmb, systemName, nthreads, n, (isRndread?"rndread":"tpcc"),cache->batch);
      }
      keepRunning = false;
   };

   if (isRndread) {
      BTree bt(envOr("VIRTGB", 4ull), envOr("PHYSGB", 1ull), envOr("THREADS", 1));

      {
         // insert
         parallel_for(0, n, nthreads, [&](uint64_t worker, uint64_t begin, uint64_t end) {
         //workerThreadId = worker;
        cache->registerThread();
            array<u8, 120> payload;
            for (u64 i=begin; i<end; i++) {
               union { u64 v1; u8 k1[sizeof(u64)]; };
               v1 = __builtin_bswap64(i);
               memcpy(payload.data(), k1, sizeof(u64));
               bt.insert({k1, sizeof(KeyType)}, payload);
            }
	        cache->forgetThread();
            //add_thread_results();
         });
      }
      cerr << "space: " << (cache->allocCount.load()*pageSize)/(float)gb << " GB " << endl;

      cache->readCount = 0;
      cache->writeCount = 0;
      thread statThread(statFn);

        parallel_for(0, nthreads, nthreads, [&](uint64_t worker, uint64_t begin, uint64_t end) {
            //workerThreadId = worker;
            cache->registerThread();
            u64 cnt = 0;
            u64 start = rdtsc();
            while (keepRunning.load()) {
                union { u64 v1; u8 k1[sizeof(u64)]; };
                v1 = __builtin_bswap64(RandomGenerator::getRand<u64>(0, n));

                array<u8, 120> payload; 
                bool succ = bt.lookup({k1, sizeof(u64)}, [&](span<u8> p) {
		        memcpy(payload.data(), p.data(), p.size());
                });
                assert(succ);
                assert(memcmp(k1, payload.data(), sizeof(u64))==0);

                cnt++;
                u64 stop = rdtsc();
                if ((stop-start) > statDiff) {
                    txProgress += cnt;
                    start = stop;
                    cnt = 0;
                }
            }
            txProgress += cnt;
	        cache->forgetThread();
            //add_thread_results();
        });

        statThread.join();
        //print_aggregate_avg();
        return 0;
    }


   // TPC-C
   Integer warehouseCount = n;

   vmcacheAdapter<warehouse_t> warehouse;
   vmcacheAdapter<district_t> district;
   vmcacheAdapter<customer_t> customer;
   customer.tree.splitOrdered = false;
   vmcacheAdapter<customer_wdl_t> customerwdl;
   vmcacheAdapter<history_t> history;
   history.tree.splitOrdered = false;
   vmcacheAdapter<neworder_t> neworder;
   vmcacheAdapter<order_t> order;
   order.tree.splitOrdered = false;
   vmcacheAdapter<order_wdc_t> order_wdc;
   vmcacheAdapter<orderline_t> orderline;
   orderline.tree.splitOrdered = false;
   vmcacheAdapter<item_t> item;
   item.tree.splitOrdered = false;
   vmcacheAdapter<stock_t> stock;
   stock.tree.splitOrdered = false;

    TPCCWorkload<vmcacheAdapter> tpcc(warehouse, district, customer, customerwdl, history, neworder, order, order_wdc, orderline, item, stock, true, warehouseCount, true);
    {
        cache->registerThread();
        tpcc.loadItem();
        tpcc.loadWarehouse();
        cache->forgetThread();
        parallel_for(1, warehouseCount+1, nthreads, [&](uint64_t worker, uint64_t begin, uint64_t end) {
            uint32_t tpcchistorycounter=0;
            //workerThreadId = worker;
	        cache->registerThread();
            for (Integer w_id=begin; w_id<end; w_id++) {
                tpcc.loadStock(w_id);
                tpcc.loadDistrinct(w_id);
                for (Integer d_id = 1; d_id <= 10; d_id++) {
                    tpcc.loadCustomer(w_id, d_id, worker, &tpcchistorycounter);
                    tpcc.loadOrders(w_id, d_id);
                }
            }
	        cache->forgetThread();
            //add_thread_results();
        });
    }
    cerr << "space: " << (cache->allocCount.load()*pageSize)/(float)gb << " GB " << endl;
    cerr << "allocCount: " << cache->allocCount.load() << ", evictCount: " << evictCount.load() << endl;
   cache->readCount = 0;
   cache->writeCount = 0;
   thread statThread(statFn);

    parallel_for(0, nthreads, nthreads, [&](uint64_t worker, uint64_t begin, uint64_t end) {
        //workerThreadId = worker;
        uint32_t tpcchistorycounter=0;
        cache->registerThread();
        u64 cnt = 0;
        u64 start = rdtsc();
        while (keepRunning.load()) {
            int w_id = tpcc.urand(1, warehouseCount); // wh crossing
            tpcc.tx(w_id, worker, &tpcchistorycounter);
            cnt++;
            u64 stop = rdtsc();
            if ((stop-start) > statDiff) {
                txProgress += cnt;
                start = stop;
                cnt = 0;
            }
        }
        txProgress += cnt;
        cache->forgetThread();
        //add_thread_results();
    });

   statThread.join();
   cerr << "space: " << (cache->allocCount.load()*pageSize)/(float)gb << " GB " << endl;
   //print_aggregate_avg();

   return 0;
}
