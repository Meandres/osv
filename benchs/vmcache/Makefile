src= ../..
CXXFLAGS = -O3 -g -DNDEBUG -DCONF_debug_memory=0 -std=c++20
CXXFLAGS += -fnon-call-exceptions -fasynchronous-unwind-tables -fPIC -shared
CXXFLAGS += -I $(src)/include -I $(src) -I $(src)/arch/common -I $(src)/arch/x64 -mavx -mavx2

module: vmcache

all: module
vmcache: vmcache.cpp tpcc/*pp
	g++ $(CXXFLAGS) vmcache.cpp -o vmcache

clean:
	rm vmcache

