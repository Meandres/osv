linux-nvme-test: nvme_test.cpp
	g++ -O3 -DLINUX nvme_test.cpp -o nvme_test

osv-nvme-test: 
src= ../..
include $(src)/modules/common.gmk

detect_arch=$(shell echo $(1) | $(CC) -E -xc- | tail -n 1)

ARCH = x64
MODE = debug

local-includes = -I.
autodepend = -MD -MT $@ -MP

COMMON += -D __BSD_VISIBLE=1
COMMON += -include $(src)/compiler/include/intrinsics.hh
COMMON += -O3 -DNDEBUG -DCONF_debug_memory=0 -DOSV
#COMMON += -g -DOSV
COMMON += -fnon-call-exceptions -fasynchronous-unwind-tables

CXXFLAGS = -std=c++20 $(COMMON) -shared -fPIC
LDFLAGS = -shared -fPIC

osv-nvme-test: nvme_test ioengine_nvme

ioengine_nvme:
	$(CXX) $(CFLAGS) $(local-includes) $(INCLUDES) -I$(src)/apps/fio/ ioengine_nvme.cc -o ioengine_nvme

nvme_test: nvme_test.cpp
	$(CXX) $(CXXFLAGS) $(local-includes) $(INCLUDES) nvme_test.cpp -o nvme_test

all: linux-vmcache

module: osv-nvme-test

clean:
	rm nvme_test ioengine_nvme

-include $(shell find -name '*.d')

