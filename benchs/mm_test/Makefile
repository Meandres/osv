linux-mmtest:
	g++ -std=c++20 -O3 mm_test.cc -o mm_test

osv-mmtest:
src= ../..
include $(src)/modules/common.gmk


ARCH = x64
MODE = release

local-includes = -I.
autodepend = -MD -MT $@ -MP

COMMON += -D __BSD_VISIBLE=1
COMMON += -include $(src)/compiler/include/intrinsics.hh
COMMON += -g -DNDEBUG -DCONF_debug_memory=0 -DOSV
COMMON += -fnon-call-exceptions -fasynchronous-unwind-tables

CXXFLAGS = -std=c++20 $(COMMON) -shared -fPIC
LDFLAGS = -shared -fPIC

osv-mmtest: mm_test.cc
	$(CXX) $(CXXFLAGS) $(local-includes) $(INCLUDES) mm_test.cc -o mm_test

all: linux-mmtest

module: mm_test

clean:
	        rm mm_test
