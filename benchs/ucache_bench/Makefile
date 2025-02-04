src= ../..
CXXFLAGS = -g -DNDEBUG -DCONF_debug_memory=0 -std=c++20 -DOSV -O3
CXXFLAGS += -fnon-call-exceptions -fasynchronous-unwind-tables -fPIC -shared
CXXFLAGS += -I $(src)/include -I $(src) -I $(src)/arch/common -I $(src)/arch/x64 -mavx -mavx2

MMAP_CXXFLAGS = -O3 -g -std=c++20 -DMMAP -mavx -mavx2
MMAP_CXXFLAGS += -fnon-call-exceptions -fasynchronous-unwind-tables -Wno-stringop-overflow

module: ucache_bench_4kib ucache_bench_16kib ucache_bench_64kib

mmap_ucache: ucache_bench.cpp tpcc/* rte_string.hh buffer_cache.hh
	g++ $(MMAP_CXXFLAGS) ucache_bench.cpp -o mmap_ucache_bench

all: module linux

ucache_bench_4kib: ucache_bench.cpp tpcc/* rte_string.hh buffer_cache.hh 
	g++ $(CXXFLAGS) -DPAGESIZE=4096 ucache_bench.cpp -o ucache_bench_4kib

ucache_bench_16kib: ucache_bench.cpp tpcc/* rte_string.hh buffer_cache.hh 
	g++ $(CXXFLAGS) -DPAGESIZE=16384 ucache_bench.cpp -o ucache_bench_16kib

ucache_bench_64kib: ucache_bench.cpp tpcc/* rte_string.hh buffer_cache.hh 
	g++ $(CXXFLAGS) -DPAGESIZE=65536 ucache_bench.cpp -o ucache_bench_64kib

clean:
	rm ucache_bench

